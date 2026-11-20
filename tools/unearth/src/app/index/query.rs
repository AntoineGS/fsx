use super::super::presentation::{
    cache_raw_record_path, can_stream_direct, can_stream_rendered, escape_terminal_text,
    final_transform_with_dirsizes, render_styled_path, style_enabled, RenderCache, RenderContext,
};
use super::*;
use regex::RegexBuilder;
use std::io::{self, BufReader, BufWriter, IsTerminal, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileTypeExt;
use std::time::Instant;

const QUERY_CONSUMER_STOP: &str = "unearth query consumer stopped after limit";
const STRICT_QUERY_MAX_SPOOL_BYTES: u64 = 256 * 1024 * 1024;
const STRICT_QUERY_OVERALL_DEADLINE: Duration = Duration::from_secs(30);

/// Writes directly to a nonblocking file descriptor so strict streaming output
/// cannot block past the daemon query's overall deadline.
struct DeadlineFdWriter {
    fd: libc::c_int,
    original_flags: libc::c_int,
    deadline: Instant,
}

impl DeadlineFdWriter {
    fn new(fd: libc::c_int, deadline: Instant) -> io::Result<Self> {
        // SAFETY: `fd` is borrowed from the caller and remains open while this
        // writer is used. F_GETFL does not mutate descriptor state.
        let original_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if original_flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: F_SETFL updates only status flags on the borrowed descriptor.
        if unsafe { libc::fcntl(fd, libc::F_SETFL, original_flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            original_flags,
            deadline,
        })
    }

    fn check_deadline(&self) -> io::Result<()> {
        if Instant::now() >= self.deadline {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "strict query timed out",
            ))
        } else {
            Ok(())
        }
    }

    fn wait_writable(&self) -> io::Result<()> {
        loop {
            self.check_deadline()?;
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            let timeout_ms = remaining
                .as_millis()
                .saturating_add(u128::from(remaining.subsec_nanos() % 1_000_000 != 0))
                .min(libc::c_int::MAX as u128) as libc::c_int;
            let mut descriptor = libc::pollfd {
                fd: self.fd,
                events: libc::POLLOUT,
                revents: 0,
            };
            // SAFETY: `descriptor` is a valid, initialized pollfd for the
            // duration of this call; poll reads/writes only that value.
            let ready = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
            if ready > 0 {
                return Ok(());
            }
            if ready == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "strict query timed out",
                ));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

impl Write for DeadlineFdWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        loop {
            self.check_deadline()?;
            // SAFETY: `buffer` is valid for `buffer.len()` bytes and `fd`
            // remains borrowed/open for this writer's lifetime. O_NONBLOCK
            // ensures this write cannot wait for output capacity.
            let written = unsafe { libc::write(self.fd, buffer.as_ptr().cast(), buffer.len()) };
            if written >= 0 {
                return Ok(written as usize);
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.kind() == io::ErrorKind::WouldBlock {
                self.wait_writable()?;
                continue;
            }
            return Err(error);
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.check_deadline()
    }
}

impl Drop for DeadlineFdWriter {
    fn drop(&mut self) {
        // SAFETY: this writer borrowed `fd` and saved its original flags in
        // `new`; restoring them here covers success and every error path.
        let _ = unsafe { libc::fcntl(self.fd, libc::F_SETFL, self.original_flags) };
    }
}

fn needs_recursive_indexed_dirsize(opts: &Options) -> bool {
    opts.sizes
        || opts.long_extended
        || matches!(opts.sort_field, Some(SortField::Size) if !opts.no_recurse)
}

/// Populate the presentation cache from a clean watcher-backed index.
///
/// A missing regular-file size makes a subtree unsafe to aggregate, so that
/// subtree is deliberately left uncached and the existing filesystem walker
/// remains the fallback.
pub(crate) fn populate_indexed_dirsize_cache(
    conn: &Connection,
    root_key: &str,
    items: &[SearchResult],
    opts: &Options,
    cache: &mut DirStatsCache,
) -> Result<(), String> {
    if !needs_recursive_indexed_dirsize(opts) {
        return Ok(());
    }

    let root_prefix = index_path_prefix(root_key);
    let mut needed_dirs = items
        .iter()
        .filter(|item| should_use_recursive_dirsize(item, opts))
        .map(|item| normalize_dir_key(&item.path))
        .filter(|path| !should_skip_root_size_tree(path))
        .filter(|path| path == root_key || path.starts_with(&root_prefix))
        .collect::<Vec<_>>();
    needed_dirs.sort_unstable();
    needed_dirs.dedup();

    let requests = fsx::hierarchy::PathRequests::new(needed_dirs.into_iter().map(PathBuf::from));
    let mut totals = vec![[0u64; 3]; requests.len()];
    let mut stmt = conn
        .prepare_cached(
            "SELECT d.path,
                 COALESCE(SUM(CASE WHEN e.kind = 0 THEN e.size ELSE 0 END), 0),
             COALESCE(SUM(CASE WHEN e.kind = 0 THEN 1 ELSE 0 END), 0),
             COALESCE(SUM(CASE WHEN e.kind = 0 AND e.size IS NULL THEN 1 ELSE 0 END), 0)
             FROM dirs d
             LEFT JOIN entries e INDEXED BY idx_entries_dir ON e.dir_id = d.id
             WHERE d.path = ?1 OR (d.path >= ?2 AND d.path < ?3)
             GROUP BY d.id",
        )
        .map_err(|e| e.to_string())?;

    for (_, path) in requests.roots() {
        let path = path.to_str().expect("index paths are encoded UTF-8");
        let prefix = index_path_prefix(path);
        let prefix_end = format!("{}0", prefix.trim_end_matches('/'));
        let mut rows = stmt
            .query(rusqlite::params![path, prefix, prefix_end])
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let parent: String = row.get(0).map_err(|e| e.to_string())?;
            let Some(id) = requests.owner(Path::new(&parent)) else {
                continue;
            };
            for column in 0..3 {
                let value: i64 = row.get(column + 1).map_err(|e| e.to_string())?;
                let value = u64::try_from(value).map_err(|_| "invalid indexed directory total")?;
                totals[id][column] = totals[id][column]
                    .checked_add(value)
                    .ok_or("indexed directory total overflow")?;
            }
        }
    }
    let mut overflow = false;
    requests.fold_children(|parent, child| {
        for column in 0..3 {
            let value = totals[child][column];
            overflow |= fsx::overflow::checked_add_u64(&mut totals[parent][column], value);
        }
    });
    if overflow {
        return Err("indexed directory total overflow".into());
    }
    for (id, path) in requests.paths() {
        let [bytes, files, missing_sizes] = totals[id];
        if missing_sizes != 0 {
            continue;
        }
        let path = path
            .to_str()
            .expect("index paths are encoded UTF-8")
            .to_owned();
        let stats = DirStats {
            files,
            bytes,
            human: format_size_iec(bytes),
        };
        cache.bytes_map.insert(path.clone(), bytes);
        cache.map.insert(path, stats);
    }
    Ok(())
}

pub(crate) fn write_binary_path_record<W: Write>(
    writer: &mut W,
    path: &str,
    path_encoded: bool,
) -> io::Result<()> {
    let mut written = 0;
    write_binary_path_record_bounded(writer, path, path_encoded, &mut written, None)
}

fn write_binary_path_record_bounded<W: Write>(
    writer: &mut W,
    path: &str,
    path_encoded: bool,
    written: &mut u64,
    max_bytes: Option<u64>,
) -> io::Result<()> {
    // SQLite stores lossless paths as escaped UTF-8 keys. Binary consumers
    // expect the original OS bytes, not the database representation.
    let decoded = path_encoded.then(|| fsx::decode_lossless_path(path));
    let bytes = decoded
        .as_deref()
        .map(|path| path.as_os_str().as_bytes())
        .unwrap_or_else(|| path.as_bytes());
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "path too long"))?;
    let record_size = 4u64 + bytes.len() as u64;
    let next_written = written
        .checked_add(record_size)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "spool size overflow"))?;
    if max_bytes.is_some_and(|max_bytes| next_written > max_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "strict query spool byte limit exceeded",
        ));
    }
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(bytes)?;
    *written = next_written;
    Ok(())
}

struct StrictBinarySpool {
    writer: BufWriter<File>,
    written_bytes: u64,
    max_bytes: u64,
}

impl StrictBinarySpool {
    fn create() -> Result<Self, String> {
        Self::create_in(&env::temp_dir())
    }

    fn create_in(directory: &Path) -> Result<Self, String> {
        let file = tempfile::tempfile_in(directory)
            .map_err(|error| format!("cannot create anonymous strict query spool: {error}"))?;
        Ok(Self {
            writer: BufWriter::with_capacity(128 * 1024, file),
            written_bytes: 0,
            max_bytes: STRICT_QUERY_MAX_SPOOL_BYTES,
        })
    }

    fn append(&mut self, result: &SearchResult) -> Result<(), String> {
        write_binary_path_record_bounded(
            &mut self.writer,
            &result.path,
            result.path_encoded,
            &mut self.written_bytes,
            Some(self.max_bytes),
        )
        .map_err(|error| error.to_string())
    }

    fn publish<W: Write>(&mut self, output: &mut W) -> Result<(), String> {
        self.writer.flush().map_err(|e| e.to_string())?;
        let file = self.writer.get_mut();
        file.seek(io::SeekFrom::Start(0))
            .map_err(|e| e.to_string())?;
        io::copy(file, output).map_err(|e| e.to_string())?;
        output.flush().map_err(|e| e.to_string())
    }
}

fn stage_strict_binary_results<W: Write>(
    reader: &mut BufReader<UnixStream>,
    output: &mut W,
    limit: Option<usize>,
    inactivity_timeout: Duration,
    deadline: Instant,
) -> Result<(), String> {
    stage_strict_binary_results_with_limits(
        reader,
        output,
        limit,
        STRICT_QUERY_MAX_SPOOL_BYTES,
        deadline,
        inactivity_timeout,
    )
}

fn stage_strict_binary_results_with_limits<W: Write>(
    reader: &mut BufReader<UnixStream>,
    output: &mut W,
    limit: Option<usize>,
    max_bytes: u64,
    deadline: Instant,
    inactivity_timeout: Duration,
) -> Result<(), String> {
    let mut spool = StrictBinarySpool::create()?;
    spool.max_bytes = max_bytes;
    let mut emitted = 0usize;
    read_query_results_with_deadline(reader, deadline, inactivity_timeout, |result| {
        if limit.is_some_and(|limit| emitted >= limit) {
            return Ok(());
        }
        spool.append(&result)?;
        emitted += 1;
        Ok(())
    })?;
    if Instant::now() >= deadline {
        return Err("strict query timed out".to_string());
    }
    spool.publish(output)
}

fn stream_strict_binary_results<W: Write>(
    reader: &mut BufReader<UnixStream>,
    output: &mut W,
    limit: Option<usize>,
    inactivity_timeout: Duration,
    deadline: Instant,
) -> Result<(), String> {
    stream_strict_binary_results_with_limits(
        reader,
        output,
        limit,
        STRICT_QUERY_MAX_SPOOL_BYTES,
        deadline,
        inactivity_timeout,
    )
}

fn stream_strict_binary_results_with_limits<W: Write>(
    reader: &mut BufReader<UnixStream>,
    output: &mut W,
    limit: Option<usize>,
    max_bytes: u64,
    deadline: Instant,
    inactivity_timeout: Duration,
) -> Result<(), String> {
    let mut emitted = 0usize;
    let mut written_bytes = 0u64;
    let mut flushed_first_record = false;
    read_query_results_with_deadline(reader, deadline, inactivity_timeout, |result| {
        if limit.is_some_and(|limit| emitted >= limit) {
            return Ok(());
        }
        write_binary_path_record_bounded(
            output,
            &result.path,
            result.path_encoded,
            &mut written_bytes,
            Some(max_bytes),
        )
        .map_err(|error| error.to_string())?;
        emitted += 1;
        if !flushed_first_record {
            output.flush().map_err(|error| error.to_string())?;
            if Instant::now() >= deadline {
                return Err("strict query timed out".to_string());
            }
            flushed_first_record = true;
        }
        Ok(())
    })?;
    if Instant::now() >= deadline {
        return Err("strict query timed out".to_string());
    }
    output.flush().map_err(|error| error.to_string())?;
    if Instant::now() >= deadline {
        return Err("strict query timed out".to_string());
    }
    Ok(())
}

fn strict_binary_output_terminal_error(stdout_is_tty: bool) -> Option<&'static str> {
    stdout_is_tty.then_some("refusing to write strict binary index results to a terminal")
}

pub(crate) fn path_has_hidden_component_below(path: &str, root: &str) -> bool {
    let path = path.trim_end_matches('/');
    let root = root.trim_end_matches('/');
    let relative = path
        .strip_prefix(root)
        .unwrap_or(path)
        .trim_start_matches('/');
    relative
        .split('/')
        .any(|part| part.len() > 1 && part.starts_with('.'))
}

pub(crate) fn indexed_root_from_opts(
    opts: &Options,
    content_spec: Option<&ContainsAllSpec>,
) -> Result<(PathBuf, Vec<String>), String> {
    if let Some(spec) = content_spec {
        return Ok((spec.root.clone(), spec.terms.clone()));
    }
    let mut terms = opts.positional.clone();
    let raw_terms = if opts.positional_os.len() == opts.positional.len() {
        Some(opts.positional_os.as_slice())
    } else {
        None
    };
    let root = if terms.len() > 1 {
        let last = terms.last().cloned().unwrap();
        let last_path = raw_terms
            .and_then(|values| values.last())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(expand_home_path(&last)));
        if is_implicit_content_path_token(&last) || last_path.is_dir() {
            terms.pop();
            last_path
        } else {
            PathBuf::from(".")
        }
    } else {
        PathBuf::from(".")
    };
    if terms.is_empty() {
        return Err("--index requires at least one search term".to_string());
    }
    Ok((root, terms))
}

pub(crate) fn recent_query_from_opts(opts: &Options) -> Result<(PathBuf, Vec<String>), String> {
    if let Some(path) = opts.path_override.as_deref() {
        return Ok((
            opts.path_override_os
                .as_ref()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(expand_home_path(path))),
            opts.positional.clone(),
        ));
    }
    let mut terms = opts.positional.clone();
    let root = terms.last().and_then(|token| {
        let raw = (opts.positional_os.len() == opts.positional.len())
            .then(|| PathBuf::from(opts.positional_os.last().expect("matching raw operand")));
        if is_implicit_content_path_token(token) || raw.as_ref().is_some_and(|path| path.is_dir()) {
            Some(raw.unwrap_or_else(|| PathBuf::from(expand_home_path(token))))
        } else {
            None
        }
    });
    if root.is_some() {
        terms.pop();
    }
    Ok((root.unwrap_or_else(|| PathBuf::from(".")), terms))
}

pub(crate) fn recent_refresh_threads(opts: &Options, root: &Path) -> usize {
    if opts.threads_explicit {
        return opts.threads_override;
    }
    if root_prefers_single_thread(root) {
        return 1;
    }
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(opts.threads_override)
        .clamp(1, 16)
}

pub(crate) fn run_recent_indexed(
    opts: &Options,
    cache: &mut DirStatsCache,
    colors: &ColorSpec,
) -> Result<SearchRun, String> {
    if let Some(result) = run_indexed_via_daemon(opts, None, cache, colors)? {
        return Ok(result);
    }
    let limit = opts
        .recent_limit
        .ok_or_else(|| "--recent requires a positive integer".to_string())?;
    let (root_raw, terms) = recent_query_from_opts(opts)?;
    let root = fs::canonicalize(&root_raw).map_err(|e| e.to_string())?;
    let root_key = normalize_index_dir(&root);
    let root_prefix = index_path_prefix(&root_key);
    let root_prefix_end = format!("{}0", root_prefix.trim_end_matches('/'));
    let conn = open_index_db_for_search()?;
    let covering_root = covering_index_root(&conn, &root_key)?;
    let refresh_root = covering_root.as_deref().unwrap_or(&root_key).to_string();
    let live = watch_state_covers_root(&conn, &root_key)?;
    drop(conn);
    if !live {
        let mut refresh_opts = opts.clone();
        refresh_opts.threads_override = recent_refresh_threads(opts, Path::new(&refresh_root));
        refresh_index_root(&refresh_root, &refresh_opts)?;
    }
    let conn = open_index_db_for_search()?;
    let fts_ready = index_search_is_ready(&conn);
    let mut regexes = Vec::with_capacity(terms.len());
    for term in &terms {
        let parsed = parse_name_pattern(term, opts.regex_mode);
        regexes.push(
            RegexBuilder::new(&parsed.regex)
                .case_insensitive(!opts.case_sensitive)
                .build()
                .map_err(|e| format!("Invalid regex: {}", e))?,
        );
    }

    let stdout_is_tty = io::stdout().is_terminal();
    let use_style = style_enabled(opts, stdout_is_tty);
    let order = opts.sort_order.unwrap_or(SortOrder::Desc);
    let order_sql = match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    let base_name_expr = "lower(s.value)";
    let full_path_expr =
        "lower((CASE WHEN d.path = '/' THEN '/' || s.value ELSE d.path || '/' || s.value END) || CASE WHEN e.kind = 1 THEN '/' ELSE '' END)";
    let sql_field_expr = if opts.force_full {
        full_path_expr
    } else {
        base_name_expr
    };
    let mut sql = String::from(
        "SELECT d.path, s.value, e.activity, e.size
         FROM entries e INDEXED BY idx_entries_kind_activity
         JOIN dirs d ON e.dir_id = d.id
         JOIN strings s ON e.name_id = s.id
         WHERE e.kind = ?
           AND e.activity IS NOT NULL
           AND (d.path = ? OR (d.path >= ? AND d.path < ?))",
    );
    let mut term_params = Vec::<String>::new();
    for term in &terms {
        if let Some((condition, extra_params)) = sql_prefilter_for_term(
            term,
            opts.regex_mode,
            opts.case_sensitive,
            opts.force_full,
            sql_field_expr,
            fts_ready,
        ) {
            sql.push_str(" AND (");
            sql.push_str(&condition);
            sql.push(')');
            term_params.extend(extra_params);
        }
    }
    sql.push_str(&format!(
        " ORDER BY e.activity {order_sql} LIMIT ? OFFSET ?"
    ));
    let mut stmt = conn.prepare_cached(&sql).map_err(|e| e.to_string())?;

    let kinds: &[i64] = if opts.force_file {
        &[0]
    } else if opts.force_dir {
        &[1]
    } else {
        &[0, 1, 2]
    };
    let mut results = Vec::with_capacity(limit.saturating_mul(kinds.len()));
    let batch_size = limit.max(64).saturating_mul(4);
    for &kind in kinds {
        let mut offset = 0usize;
        let mut visible_for_kind = 0usize;
        while visible_for_kind < limit {
            let mut query_params = Vec::<rusqlite::types::Value>::with_capacity(
                6usize.saturating_add(term_params.len()),
            );
            query_params.push(kind.into());
            query_params.push(root_key.clone().into());
            query_params.push(root_prefix.clone().into());
            query_params.push(root_prefix_end.clone().into());
            query_params.extend(term_params.iter().cloned().map(Into::into));
            query_params.push(
                i64::try_from(batch_size)
                    .map_err(|_| "recent batch too large".to_string())?
                    .into(),
            );
            query_params.push(
                i64::try_from(offset)
                    .map_err(|_| "recent offset too large".to_string())?
                    .into(),
            );
            let rows = stmt
                .query_map(params_from_iter(query_params.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                })
                .map_err(|e| e.to_string())?;

            let mut fetched = 0usize;
            for row in rows {
                let (dir_path, name, activity, size) = row.map_err(|e| e.to_string())?;
                fetched += 1;
                let mut path = if dir_path == "/" {
                    format!("/{}", name)
                } else {
                    format!("{}/{}", dir_path, name)
                };
                if kind == 1 {
                    path.push('/');
                }
                if is_root_index_excluded_path(&root_key, &path) {
                    continue;
                }
                if opts.visible_only && path_has_hidden_component_below(&path, &root_key) {
                    continue;
                }
                let base = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
                let matched = if opts.force_full {
                    regexes.iter().all(|regex| regex.is_match(&path))
                } else {
                    regexes.iter().all(|regex| regex.is_match(base))
                };
                if !matched
                    || (opts.force_full
                        && regexes.len() > 1
                        && !regexes.iter().any(|regex| regex.is_match(base)))
                {
                    continue;
                }
                results.push(SearchResult {
                    path,
                    path_encoded: true,
                    is_dir: kind == 1,
                    is_symlink: kind == 2,
                    metadata: None,
                    indexed_activity_nanos: activity,
                    indexed_size: size.and_then(|value| u64::try_from(value).ok()),
                });
                visible_for_kind += 1;
                if visible_for_kind >= limit {
                    break;
                }
            }
            if fetched < batch_size {
                break;
            }
            offset = offset.saturating_add(batch_size);
        }
    }
    results.sort_unstable_by(|a, b| {
        let a_activity = a.indexed_activity_nanos.unwrap_or(0);
        let b_activity = b.indexed_activity_nanos.unwrap_or(0);
        match order {
            SortOrder::Asc => a_activity
                .cmp(&b_activity)
                .then_with(|| a.is_dir.cmp(&b.is_dir))
                .then_with(|| a.path.cmp(&b.path)),
            SortOrder::Desc => b_activity
                .cmp(&a_activity)
                .then_with(|| a.is_dir.cmp(&b.is_dir))
                .then_with(|| a.path.cmp(&b.path)),
        }
    });
    results.truncate(limit);

    Ok(SearchRun {
        lines: final_transform_with_dirsizes(
            results,
            opts,
            use_style,
            stdout_is_tty,
            colors,
            cache,
            None,
            |items, cache| {
                if live {
                    let _ = populate_indexed_dirsize_cache(&conn, &root_key, items, opts, cache);
                }
            },
        ),
        timed_out: false,
        incomplete: false,
    })
}

#[cfg(feature = "watcher")]
pub(crate) fn query_recent_rows<F>(
    opts: &Options,
    conn: &Connection,
    mut emit: F,
) -> Result<(), String>
where
    F: FnMut(SearchResult) -> Result<(), String>,
{
    let limit = opts
        .recent_limit
        .ok_or_else(|| "--recent requires a positive integer".to_string())?;
    let (root_raw, terms) = recent_query_from_opts(opts)?;
    let root = fs::canonicalize(&root_raw).map_err(|e| e.to_string())?;
    let root_key = normalize_index_dir(&root);
    let root_prefix = index_path_prefix(&root_key);
    let root_prefix_end = format!("{}0", root_prefix.trim_end_matches('/'));
    let fts_ready = index_search_is_ready(conn);
    let mut regexes = Vec::with_capacity(terms.len());
    for term in &terms {
        let parsed = parse_name_pattern(term, opts.regex_mode);
        regexes.push(
            RegexBuilder::new(&parsed.regex)
                .case_insensitive(!opts.case_sensitive)
                .build()
                .map_err(|e| format!("Invalid regex: {}", e))?,
        );
    }
    let order = opts.sort_order.unwrap_or(SortOrder::Desc);
    let order_sql = match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    let base_name_expr = "lower(s.value)";
    let full_path_expr =
        "lower((CASE WHEN d.path = '/' THEN '/' || s.value ELSE d.path || '/' || s.value END) || CASE WHEN e.kind = 1 THEN '/' ELSE '' END)";
    let sql_field_expr = if opts.force_full {
        full_path_expr
    } else {
        base_name_expr
    };
    let mut sql = String::from(
        "SELECT d.path, s.value, e.activity, e.size
         FROM entries e INDEXED BY idx_entries_kind_activity
         JOIN dirs d ON e.dir_id = d.id
         JOIN strings s ON e.name_id = s.id
         WHERE e.kind = ?
           AND e.activity IS NOT NULL
           AND (d.path = ? OR (d.path >= ? AND d.path < ?))",
    );
    let mut term_params = Vec::<String>::new();
    for term in &terms {
        if let Some((condition, extra_params)) = sql_prefilter_for_term(
            term,
            opts.regex_mode,
            opts.case_sensitive,
            opts.force_full,
            sql_field_expr,
            fts_ready,
        ) {
            sql.push_str(" AND (");
            sql.push_str(&condition);
            sql.push(')');
            term_params.extend(extra_params);
        }
    }
    sql.push_str(&format!(
        " ORDER BY e.activity {order_sql} LIMIT ? OFFSET ?"
    ));
    let mut stmt = conn.prepare_cached(&sql).map_err(|e| e.to_string())?;
    let kinds: &[i64] = if opts.force_file {
        &[0]
    } else if opts.force_dir {
        &[1]
    } else {
        &[0, 1, 2]
    };
    let batch_size = limit.max(64).saturating_mul(4);
    for &kind in kinds {
        let mut offset = 0usize;
        let mut visible_for_kind = 0usize;
        while visible_for_kind < limit {
            let mut query_params = Vec::<rusqlite::types::Value>::with_capacity(
                6usize.saturating_add(term_params.len()),
            );
            query_params.push(kind.into());
            query_params.push(root_key.clone().into());
            query_params.push(root_prefix.clone().into());
            query_params.push(root_prefix_end.clone().into());
            query_params.extend(term_params.iter().cloned().map(Into::into));
            query_params.push(
                i64::try_from(batch_size)
                    .map_err(|_| "recent batch too large".to_string())?
                    .into(),
            );
            query_params.push(
                i64::try_from(offset)
                    .map_err(|_| "recent offset too large".to_string())?
                    .into(),
            );
            let rows = stmt
                .query_map(params_from_iter(query_params.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                })
                .map_err(|e| e.to_string())?;
            let mut fetched = 0usize;
            for row in rows {
                let (dir_path, name, activity, size) = row.map_err(|e| e.to_string())?;
                fetched += 1;
                let mut path = if dir_path == "/" {
                    format!("/{}", name)
                } else {
                    format!("{}/{}", dir_path, name)
                };
                if kind == 1 {
                    path.push('/');
                }
                if is_root_index_excluded_path(&root_key, &path) {
                    continue;
                }
                if opts.visible_only && path_has_hidden_component_below(&path, &root_key) {
                    continue;
                }
                let base = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
                let matched = if opts.force_full {
                    regexes.iter().all(|regex| regex.is_match(&path))
                } else {
                    regexes.iter().all(|regex| regex.is_match(base))
                };
                if !matched
                    || (opts.force_full
                        && regexes.len() > 1
                        && !regexes.iter().any(|regex| regex.is_match(base)))
                {
                    continue;
                }
                emit(SearchResult {
                    path,
                    path_encoded: true,
                    is_dir: kind == 1,
                    is_symlink: kind == 2,
                    metadata: None,
                    indexed_activity_nanos: activity,
                    indexed_size: size.and_then(|value| u64::try_from(value).ok()),
                })?;
                visible_for_kind += 1;
                if visible_for_kind >= limit {
                    break;
                }
            }
            if fetched < batch_size {
                break;
            }
            offset = offset.saturating_add(batch_size);
        }
    }
    Ok(())
}

#[cfg(feature = "watcher")]
pub(crate) fn query_indexed_rows<F>(
    opts: &Options,
    content_spec: Option<&ContainsAllSpec>,
    conn: &Connection,
    mut emit: F,
) -> Result<(), String>
where
    F: FnMut(SearchResult) -> Result<(), String>,
{
    let (root_raw, terms) = indexed_root_from_opts(opts, content_spec)?;
    let root = fs::canonicalize(&root_raw).map_err(|e| e.to_string())?;
    let root_key = normalize_index_dir(&root);
    let root_prefix = index_path_prefix(&root_key);
    let mut type_flag = if opts.force_dir {
        Some(TypeFlag::Dir)
    } else if opts.force_file {
        Some(TypeFlag::File)
    } else {
        None
    };
    let mut regexes = Vec::new();
    for term in &terms {
        let parsed = parse_name_pattern(term, opts.regex_mode);
        if parsed.type_flag == Some(TypeFlag::Dir) && !opts.force_file {
            type_flag = Some(TypeFlag::Dir);
        }
        regexes.push(
            RegexBuilder::new(&parsed.regex)
                .case_insensitive(!opts.case_sensitive)
                .build()
                .map_err(|e| format!("Invalid regex: {}", e))?,
        );
    }
    let fts_ready = index_search_is_ready(conn);
    let base_name_expr = "lower(s.value)";
    let full_path_expr =
        "lower((CASE WHEN d.path = '/' THEN '/' || s.value ELSE d.path || '/' || s.value END) || CASE WHEN e.kind = 1 THEN '/' ELSE '' END)";
    let sql_field_expr = if opts.force_full {
        full_path_expr
    } else {
        base_name_expr
    };
    let root_prefix_end = format!("{}0", root_prefix.trim_end_matches('/'));
    let mut sql = String::from(
        "SELECT d.path, s.value, e.kind, e.mtime, e.size, e.activity
         FROM entries e
         JOIN dirs d ON e.dir_id = d.id
         JOIN strings s ON e.name_id = s.id
         WHERE (d.path = ? OR (d.path >= ? AND d.path < ?))",
    );
    let mut sql_params = vec![root_key.clone(), root_prefix.clone(), root_prefix_end];
    match type_flag {
        Some(TypeFlag::Dir) => sql.push_str(" AND e.kind = 1"),
        Some(TypeFlag::File) => sql.push_str(" AND e.kind != 1"),
        None => {}
    }
    for term in &terms {
        if let Some((condition, extra_params)) = sql_prefilter_for_term(
            term,
            opts.regex_mode,
            opts.case_sensitive,
            opts.force_full,
            sql_field_expr,
            fts_ready,
        ) {
            sql.push_str(" AND ");
            sql.push('(');
            sql.push_str(&condition);
            sql.push(')');
            sql_params.extend(extra_params);
        }
    }
    let mut stmt = conn.prepare_cached(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params_from_iter(sql_params.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })
        .map_err(|e| e.to_string())?;

    for row in rows {
        let (dir_path, name, kind, mtime, size, activity) = row.map_err(|e| e.to_string())?;
        let is_dir = kind == 1;
        let is_symlink = kind == 2;
        if matches!(type_flag, Some(TypeFlag::Dir)) && !is_dir {
            continue;
        }
        if matches!(type_flag, Some(TypeFlag::File)) && is_dir {
            continue;
        }
        let mut path = if dir_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", dir_path, name)
        };
        if is_dir {
            path.push('/');
        }
        if is_root_index_excluded_path(&root_key, &path) {
            continue;
        }
        if opts.visible_only && path_has_hidden_component_below(&path, &root_key) {
            continue;
        }
        let base = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
        let matched = if opts.force_full {
            regexes.iter().all(|regex| regex.is_match(&path))
        } else {
            regexes.iter().all(|regex| regex.is_match(base))
        };
        if !matched
            || (opts.force_full
                && regexes.len() > 1
                && !regexes.iter().any(|regex| regex.is_match(base)))
        {
            continue;
        }
        emit(SearchResult {
            path,
            path_encoded: true,
            is_dir,
            is_symlink,
            metadata: None,
            indexed_activity_nanos: activity.or(mtime),
            indexed_size: size.and_then(|value| u64::try_from(value).ok()),
        })?;
    }
    Ok(())
}

pub(crate) fn begin_query_results(
    stream: UnixStream,
) -> Result<Option<BufReader<UnixStream>>, String> {
    begin_query_results_inner(stream, None)
}

fn begin_query_results_with_deadline(
    stream: UnixStream,
    deadline: Instant,
    inactivity_timeout: Duration,
) -> Result<Option<BufReader<UnixStream>>, String> {
    begin_query_results_inner(stream, Some((deadline, inactivity_timeout)))
}

fn begin_query_results_inner(
    stream: UnixStream,
    timeout: Option<(Instant, Duration)>,
) -> Result<Option<BufReader<UnixStream>>, String> {
    let mut reader = BufReader::new(stream);
    let mut magic = [0u8; 8];
    let magic_result = match timeout {
        Some(timeout) => read_query_exact(&mut reader, &mut magic, Some(timeout)),
        None => reader.read_exact(&mut magic),
    };
    if let Err(error) = magic_result {
        if timeout.is_some() {
            return Err(error.to_string());
        }
        return Ok(None);
    }
    if &magic != QUERY_RESPONSE_MAGIC {
        return Ok(None);
    }
    let mut status = [0u8; 1];
    let status_result = match timeout {
        Some(timeout) => read_query_exact(&mut reader, &mut status, Some(timeout)),
        None => reader.read_exact(&mut status),
    };
    if let Err(error) = status_result {
        if timeout.is_some() {
            return Err(error.to_string());
        }
        return Ok(None);
    }
    if status[0] != 0 {
        if let Some(timeout) = timeout {
            let mut length = [0u8; 4];
            read_query_exact(&mut reader, &mut length, Some(timeout))
                .map_err(|error| error.to_string())?;
            let length = u32::from_le_bytes(length) as usize;
            if length > QUERY_MAX_FRAME {
                return Err("query string exceeds protocol limit".to_string());
            }
            let mut bytes = vec![0u8; length];
            read_query_exact(&mut reader, &mut bytes, Some(timeout))
                .map_err(|error| error.to_string())?;
            return Err(String::from_utf8(bytes).map_err(|error| error.to_string())?);
        }
        return Err(read_query_string(&mut reader)?);
    }
    Ok(Some(reader))
}

pub(crate) fn read_query_results<F>(
    reader: &mut BufReader<UnixStream>,
    emit: F,
) -> Result<(), String>
where
    F: FnMut(SearchResult) -> Result<(), String>,
{
    read_query_results_inner(reader, emit, None)
}

fn read_query_results_with_deadline<F>(
    reader: &mut BufReader<UnixStream>,
    deadline: Instant,
    inactivity_timeout: Duration,
    emit: F,
) -> Result<(), String>
where
    F: FnMut(SearchResult) -> Result<(), String>,
{
    read_query_results_inner(reader, emit, Some((deadline, inactivity_timeout)))
}

fn read_query_results_inner<F>(
    reader: &mut BufReader<UnixStream>,
    mut emit: F,
    timeout: Option<(Instant, Duration)>,
) -> Result<(), String>
where
    F: FnMut(SearchResult) -> Result<(), String>,
{
    loop {
        let mut length = [0u8; 4];
        read_query_exact(reader, &mut length, timeout).map_err(|e| e.to_string())?;
        let length = u32::from_le_bytes(length) as usize;
        if length == 0 {
            break;
        }
        if !(17..=QUERY_MAX_FRAME).contains(&length) {
            return Err("invalid fsxd query result frame".to_string());
        }
        let mut frame = vec![0u8; length];
        read_query_exact(reader, &mut frame, timeout).map_err(|e| e.to_string())?;
        let is_dir = frame[0] & 1 != 0;
        let is_symlink = frame[0] & 2 != 0;
        let activity = i64::from_le_bytes(
            frame[1..9]
                .try_into()
                .map_err(|_| "invalid activity field".to_string())?,
        );
        let size = i64::from_le_bytes(
            frame[9..17]
                .try_into()
                .map_err(|_| "invalid size field".to_string())?,
        );
        let path = String::from_utf8(frame[17..].to_vec()).map_err(|e| e.to_string())?;
        emit(SearchResult {
            path,
            path_encoded: true,
            is_dir,
            is_symlink,
            metadata: None,
            indexed_activity_nanos: (activity != i64::MIN).then_some(activity),
            indexed_size: (size >= 0 && size != i64::MIN).then_some(size as u64),
        })?;
    }
    Ok(())
}

fn read_query_exact(
    reader: &mut BufReader<UnixStream>,
    buffer: &mut [u8],
    timeout: Option<(Instant, Duration)>,
) -> io::Result<()> {
    let Some((deadline, inactivity_timeout)) = timeout else {
        return reader.read_exact(buffer);
    };

    let mut offset = 0;
    while offset < buffer.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "strict query timed out",
            ));
        }
        if reader.buffer().is_empty() {
            reader
                .get_ref()
                .set_read_timeout(Some(inactivity_timeout.min(remaining)))?;
        }
        match reader.read(&mut buffer[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected end of strict query response",
                ))
            }
            Ok(read) => offset += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "strict query timed out",
                ))
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(crate) fn run_indexed_via_daemon(
    opts: &Options,
    content_spec: Option<&ContainsAllSpec>,
    cache: &mut DirStatsCache,
    colors: &ColorSpec,
) -> Result<Option<SearchRun>, String> {
    if opts.require_live_index {
        if let Some(error) = strict_binary_output_terminal_error(io::stdout().is_terminal()) {
            return Err(error.to_string());
        }
    }
    /* The daemon protocol predates case-sensitive requests. Use the local
     * indexed query path until that protocol grows a versioned flag. */
    if opts.case_sensitive && !opts.require_live_index {
        return Ok(None);
    }

    let socket =
        query_socket_path().ok_or_else(|| "Could not determine fsx cache dir".to_string())?;
    if !fs::symlink_metadata(&socket)
        .map(|metadata| metadata.file_type().is_socket())
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let (root_raw, terms) = if opts.recent_limit.is_some() {
        recent_query_from_opts(opts)?
    } else {
        indexed_root_from_opts(opts, content_spec)?
    };
    let root = fs::canonicalize(&root_raw).map_err(|e| e.to_string())?;
    let root_key = normalize_index_dir(&root);
    let status_conn = if opts.require_live_index {
        None
    } else {
        let conn = match open_index_db_for_search() {
            Ok(conn) => conn,
            Err(_) => return Ok(None),
        };
        if !watch_state_covers_root(&conn, &root_key)? {
            return Ok(None);
        }
        Some(conn)
    };
    let mut stream = match UnixStream::connect(&socket) {
        Ok(stream) => stream,
        Err(_) => return Ok(None),
    };
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(opts.timeout_dur))
        .map_err(|e| e.to_string())?;
    let strict_deadline = opts
        .require_live_index
        .then(|| Instant::now() + STRICT_QUERY_OVERALL_DEADLINE);
    if write_query_request(&mut stream, opts, &root, &terms).is_err() {
        return Ok(None);
    }
    let reader = match strict_deadline {
        Some(deadline) => begin_query_results_with_deadline(stream, deadline, opts.timeout_dur)?,
        None => begin_query_results(stream)?,
    };
    let mut reader = match reader {
        Some(reader) => reader,
        None => return Ok(None),
    };
    if opts.require_live_index {
        let deadline = strict_deadline.expect("strict request has a deadline");
        if opts.stream_index_binary {
            let stdout = io::stdout();
            let stdout_lock = stdout.lock();
            let mut output = DeadlineFdWriter::new(stdout_lock.as_raw_fd(), deadline)
                .map_err(|error| error.to_string())?;
            stream_strict_binary_results(
                &mut reader,
                &mut output,
                opts.limit,
                opts.timeout_dur,
                deadline,
            )?;
        } else {
            let stdout = io::stdout();
            let mut output = BufWriter::with_capacity(64 * 1024, stdout.lock());
            stage_strict_binary_results(
                &mut reader,
                &mut output,
                opts.limit,
                opts.timeout_dur,
                deadline,
            )?;
        }
        return Ok(Some(SearchRun {
            lines: Vec::new(),
            timed_out: false,
            incomplete: false,
        }));
    }
    let stdout_is_tty = io::stdout().is_terminal();
    let use_style = style_enabled(opts, stdout_is_tty);
    if opts.recent_limit.is_none() && can_stream_direct(opts, use_style) {
        let stdout = io::stdout();
        let mut output = BufWriter::with_capacity(64 * 1024, stdout.lock());
        let mut cache_state = if opts.cache_output {
            init_raw_cache_state()
        } else {
            None
        };
        let mut emitted = 0usize;
        let result = read_query_results(&mut reader, |result| {
            if opts.limit.is_some_and(|limit| emitted >= limit) {
                return Err(QUERY_CONSUMER_STOP.to_string());
            }
            if let Some(state) = cache_state.as_mut() {
                cache_raw_record_path(&result.path, result.is_dir, result.path_encoded, state);
            }
            if opts.index_binary {
                write_binary_path_record(&mut output, &result.path, result.path_encoded)
                    .map_err(|e| e.to_string())?;
            } else {
                let display_path = if opts.lossless_paths && !result.path_encoded {
                    fsx::encode_lossless_path(Path::new(&result.path))
                } else {
                    result.path.clone()
                };
                output
                    .write_all(escape_terminal_text(&display_path).as_bytes())
                    .and_then(|_| output.write_all(b"\n"))
                    .map_err(|e| e.to_string())?;
            }
            emitted += 1;
            Ok(())
        });
        if let Err(error) = result {
            if error != QUERY_CONSUMER_STOP {
                return Err(format!("indexed query failed after output: {error}"));
            }
        }
        output.flush().map_err(|e| e.to_string())?;
        if let Some(mut state) = cache_state {
            let _ = state.cache.flush();
        }
        return Ok(Some(SearchRun {
            lines: Vec::new(),
            timed_out: false,
            incomplete: false,
        }));
    }
    if can_stream_rendered(opts) {
        let stdout = io::stdout();
        let mut output = BufWriter::with_capacity(64 * 1024, stdout.lock());
        let mut render_cache = RenderCache::default();
        let mut cache_state = opts.cache_output.then(init_raw_cache_state).flatten();
        let mut context = RenderContext {
            use_style,
            add_decorator: stdout_is_tty || opts.classify,
            colors,
            opts,
            highlight: None,
            cache: &mut render_cache,
        };
        let mut emitted = 0usize;
        let result = read_query_results(&mut reader, |row| {
            if let Some(state) = cache_state.as_mut() {
                cache_raw_record_path(&row.path, row.is_dir, row.path_encoded, state);
            }
            if opts.limit.is_some_and(|limit| emitted >= limit) {
                return if opts.cache_output {
                    Ok(())
                } else {
                    Err(QUERY_CONSUMER_STOP.to_string())
                };
            }
            writeln!(output, "{}", render_styled_path(&row, &mut context))
                .map_err(|e| e.to_string())?;
            emitted += 1;
            if emitted == 1 {
                output.flush().map_err(|e| e.to_string())?;
            }
            Ok(())
        });
        if let Err(error) = result {
            if error != QUERY_CONSUMER_STOP {
                return Err(format!("indexed query failed after output: {error}"));
            }
        }
        output.flush().map_err(|e| e.to_string())?;
        if let Some(mut state) = cache_state {
            let _ = state.cache.flush();
        }
        return Ok(Some(SearchRun {
            lines: Vec::new(),
            timed_out: false,
            incomplete: false,
        }));
    }
    let mut results = Vec::new();
    read_query_results(&mut reader, |result| {
        results.push(result);
        Ok(())
    })
    .map_err(|error| format!("indexed query failed: {error}"))?;
    if let Some(limit) = opts.recent_limit {
        let order = opts.sort_order.unwrap_or(SortOrder::Desc);
        results.sort_unstable_by(|a, b| {
            let a_activity = a.indexed_activity_nanos.unwrap_or(0);
            let b_activity = b.indexed_activity_nanos.unwrap_or(0);
            match order {
                SortOrder::Asc => a_activity
                    .cmp(&b_activity)
                    .then_with(|| a.is_dir.cmp(&b.is_dir))
                    .then_with(|| a.path.cmp(&b.path)),
                SortOrder::Desc => b_activity
                    .cmp(&a_activity)
                    .then_with(|| a.is_dir.cmp(&b.is_dir))
                    .then_with(|| a.path.cmp(&b.path)),
            }
        });
        results.truncate(limit);
    } else {
        results.sort_by(|a, b| a.path.cmp(&b.path));
    }
    Ok(Some(SearchRun {
        lines: final_transform_with_dirsizes(
            results,
            opts,
            use_style,
            stdout_is_tty,
            colors,
            cache,
            None,
            |items, cache| {
                if let Some(status_conn) = status_conn.as_ref() {
                    let _ =
                        populate_indexed_dirsize_cache(status_conn, &root_key, items, opts, cache);
                }
            },
        ),
        timed_out: false,
        incomplete: false,
    }))
}

pub(crate) fn run_indexed(
    opts: &Options,
    content_spec: Option<&ContainsAllSpec>,
    cache: &mut DirStatsCache,
    colors: &ColorSpec,
) -> Result<SearchRun, String> {
    if let Some(result) = run_indexed_via_daemon(opts, content_spec, cache, colors)? {
        return Ok(result);
    }
    let (root_raw, terms) = indexed_root_from_opts(opts, content_spec)?;
    let root = fs::canonicalize(&root_raw).map_err(|e| e.to_string())?;
    let root_key = normalize_index_dir(&root);
    let root_prefix = index_path_prefix(&root_key);
    let mut type_flag = if opts.force_dir {
        Some(TypeFlag::Dir)
    } else if opts.force_file {
        Some(TypeFlag::File)
    } else {
        None
    };
    let mut regexes = Vec::new();
    for term in &terms {
        let parsed = parse_name_pattern(term, opts.regex_mode);
        if parsed.type_flag == Some(TypeFlag::Dir) && !opts.force_file {
            type_flag = Some(TypeFlag::Dir);
        }
        regexes.push(
            RegexBuilder::new(&parsed.regex)
                .case_insensitive(!opts.case_sensitive)
                .build()
                .map_err(|e| format!("Invalid regex: {}", e))?,
        );
    }
    let conn = open_index_db_for_search()?;
    let fts_ready = index_search_is_ready(&conn);
    let covering_root = covering_index_root(&conn, &root_key)?;
    let refresh_root = covering_root.as_deref().unwrap_or(&root_key);
    let clean_index = watch_state_covers_root(&conn, &root_key)?;
    if !clean_index {
        spawn_index_refresh(refresh_root, opts, covering_root.is_none());
    }
    let stdout_is_tty = io::stdout().is_terminal();
    let use_style = style_enabled(opts, stdout_is_tty);
    let base_name_expr = "lower(s.value)";
    let full_path_expr =
        "lower((CASE WHEN d.path = '/' THEN '/' || s.value ELSE d.path || '/' || s.value END) || CASE WHEN e.kind = 1 THEN '/' ELSE '' END)";
    let sql_field_expr = if opts.force_full {
        full_path_expr
    } else {
        base_name_expr
    };
    let root_prefix_end = format!("{}0", root_prefix.trim_end_matches('/'));
    let mut sql = String::from(
        "SELECT d.path, s.value, e.kind, e.mtime, e.size, e.activity
         FROM entries e
         JOIN dirs d ON e.dir_id = d.id
         JOIN strings s ON e.name_id = s.id
         WHERE (d.path = ? OR (d.path >= ? AND d.path < ?))",
    );
    let mut sql_params = vec![root_key.clone(), root_prefix.clone(), root_prefix_end];
    match type_flag {
        Some(TypeFlag::Dir) => sql.push_str(" AND e.kind = 1"),
        Some(TypeFlag::File) => sql.push_str(" AND e.kind != 1"),
        None => {}
    }
    for term in &terms {
        if let Some((condition, extra_params)) = sql_prefilter_for_term(
            term,
            opts.regex_mode,
            opts.case_sensitive,
            opts.force_full,
            sql_field_expr,
            fts_ready,
        ) {
            sql.push_str(" AND ");
            sql.push('(');
            sql.push_str(&condition);
            sql.push(')');
            sql_params.extend(extra_params);
        }
    }
    let render_stream = can_stream_rendered(opts) && !can_stream_direct(opts, use_style);
    if render_stream {
        // Buffered styled output was lexically sorted by the complete path.
        // Preserve that ordering while letting SQLite spill sorting to disk.
        sql.push_str(" ORDER BY (CASE WHEN d.path = '/' THEN '/' || s.value ELSE d.path || '/' || s.value END) || CASE WHEN e.kind = 1 THEN '/' ELSE '' END COLLATE BINARY");
    }
    let mut stmt = conn.prepare_cached(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params_from_iter(sql_params.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })
        .map_err(|e| e.to_string())?;

    if can_stream_direct(opts, use_style) || render_stream {
        let stdout = io::stdout();
        let mut lock = BufWriter::with_capacity(64 * 1024, stdout.lock());
        let mut render_cache = RenderCache::default();
        let mut context = RenderContext {
            use_style,
            add_decorator: stdout_is_tty || opts.classify,
            colors,
            opts,
            highlight: None,
            cache: &mut render_cache,
        };
        let mut cache_state = if opts.cache_output {
            init_raw_cache_state()
        } else {
            None
        };
        let mut written_since_flush = 0usize;
        let mut emitted = 0usize;

        for row in rows {
            let (dir_path, name, kind, _mtime, _size, _activity) =
                row.map_err(|e| e.to_string())?;
            let is_dir = kind == 1;
            if matches!(type_flag, Some(TypeFlag::Dir)) && !is_dir {
                continue;
            }
            if matches!(type_flag, Some(TypeFlag::File)) && is_dir {
                continue;
            }
            let mut path = if dir_path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", dir_path, name)
            };
            if is_dir {
                path.push('/');
            }
            if is_root_index_excluded_path(&root_key, &path) {
                continue;
            }
            if opts.visible_only && path_has_hidden_component_below(&path, &root_key) {
                continue;
            }
            let base = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
            let matched = if opts.force_full {
                regexes.iter().all(|re| re.is_match(&path))
            } else {
                regexes.iter().all(|re| re.is_match(base))
            };
            if !matched {
                continue;
            }
            if opts.force_full && regexes.len() > 1 && !regexes.iter().any(|re| re.is_match(base)) {
                continue;
            }
            let limited = opts.limit.is_some_and(|limit| emitted >= limit);
            if limited && !(render_stream && opts.cache_output) {
                break;
            }
            if let Some(state) = cache_state.as_mut() {
                cache_raw_record_path(&path, is_dir, true, state);
            }
            if limited {
                continue;
            }
            if render_stream {
                let row = SearchResult {
                    path,
                    path_encoded: true,
                    is_dir,
                    is_symlink: kind == 2,
                    metadata: None,
                    indexed_size: None,
                    indexed_activity_nanos: None,
                };
                writeln!(lock, "{}", render_styled_path(&row, &mut context))
                    .map_err(|e| e.to_string())?;
            } else if opts.index_binary {
                write_binary_path_record(&mut lock, &path, true).map_err(|e| e.to_string())?;
            } else {
                let display_path = escape_terminal_text(&path);
                lock.write_all(display_path.as_bytes())
                    .and_then(|_| lock.write_all(b"\n"))
                    .map_err(|e| e.to_string())?;
            }
            emitted += 1;
            written_since_flush += 1;
            if emitted == 1 || written_since_flush >= INDEX_STREAM_FLUSH_LINES {
                lock.flush().map_err(|e| e.to_string())?;
                written_since_flush = 0;
            }
        }
        lock.flush().map_err(|e| e.to_string())?;
        if let Some(mut state) = cache_state {
            let _ = state.cache.flush();
        }
        return Ok(SearchRun {
            lines: Vec::new(),
            timed_out: false,
            incomplete: false,
        });
    }

    let mut results = Vec::new();
    for row in rows {
        let (dir_path, name, kind, mtime, size, activity) = row.map_err(|e| e.to_string())?;
        let is_dir = kind == 1;
        let is_symlink = kind == 2;
        if matches!(type_flag, Some(TypeFlag::Dir)) && !is_dir {
            continue;
        }
        if matches!(type_flag, Some(TypeFlag::File)) && is_dir {
            continue;
        }
        let mut path = if dir_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", dir_path, name)
        };
        if is_dir {
            path.push('/');
        }
        if is_root_index_excluded_path(&root_key, &path) {
            continue;
        }
        if opts.visible_only && path_has_hidden_component_below(&path, &root_key) {
            continue;
        }
        let base = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
        let matched = if opts.force_full {
            regexes.iter().all(|re| re.is_match(&path))
        } else {
            regexes.iter().all(|re| re.is_match(base))
        };
        if !matched {
            continue;
        }
        if opts.force_full && regexes.len() > 1 && !regexes.iter().any(|re| re.is_match(base)) {
            continue;
        }
        results.push(SearchResult {
            path,
            path_encoded: true,
            is_dir,
            is_symlink,
            metadata: None,
            indexed_activity_nanos: activity.or(mtime),
            indexed_size: size.and_then(|value| u64::try_from(value).ok()),
        });
    }
    results.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(SearchRun {
        lines: final_transform_with_dirsizes(
            results,
            opts,
            use_style,
            stdout_is_tty,
            colors,
            cache,
            None,
            |items, cache| {
                if clean_index {
                    let _ = populate_indexed_dirsize_cache(&conn, &root_key, items, opts, cache);
                }
            },
        ),
        timed_out: false,
        incomplete: false,
    })
}

pub(crate) fn clean_watcher_covers_search(
    opts: &Options,
    content_spec: Option<&ContainsAllSpec>,
) -> Result<bool, String> {
    let (root_raw, _) = indexed_root_from_opts(opts, content_spec)?;
    let root = fs::canonicalize(&root_raw).map_err(|e| e.to_string())?;
    let root_key = normalize_index_dir(&root);
    let conn = open_index_db_for_search()?;
    watch_state_covers_root(&conn, &root_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn strict_binary_output_rejects_a_terminal() {
        assert_eq!(
            strict_binary_output_terminal_error(true),
            Some("refusing to write strict binary index results to a terminal")
        );
        assert_eq!(strict_binary_output_terminal_error(false), None);
    }

    #[cfg(unix)]
    #[test]
    fn strict_staging_honors_limit_but_consumes_the_terminator() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        write_test_query_result(&mut server, b"/first");
        write_test_query_result(&mut server, b"/second");
        server.write_all(&0u32.to_le_bytes()).expect("terminator");
        drop(server);

        let mut reader = BufReader::new(client);
        let mut output = Vec::new();
        stage_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            Some(1),
            1024,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .expect("stage limited query");

        let mut expected = Vec::new();
        write_binary_path_record(&mut expected, "/first", true).expect("expected record");
        assert_eq!(output, expected);
        let mut trailing = [0; 1];
        assert_eq!(reader.read(&mut trailing).expect("terminator consumed"), 0);
    }

    #[cfg(unix)]
    #[test]
    fn strict_staging_rejects_record_before_exceeding_byte_budget() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        write_test_query_result(&mut server, b"/too-large");
        let mut reader = BufReader::new(client);
        let mut output = Vec::new();

        let result = stage_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            4 + b"/too-large".len() as u64 - 1,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        );

        assert!(result.unwrap_err().contains("byte limit"));
        assert!(output.is_empty(), "over-budget data was published");
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_flushes_records_before_the_daemon_terminator() {
        use std::sync::mpsc;

        let (mut server, client) = UnixStream::pair().expect("socket pair");
        let (flushed_tx, flushed_rx) = mpsc::channel();
        let sender = std::thread::spawn(move || {
            write_test_query_result(&mut server, b"/first");
            let visible = flushed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first record flushed before terminator");
            let mut expected = Vec::new();
            write_binary_path_record(&mut expected, "/first", true).expect("expected record");
            assert_eq!(visible, expected);
            server.write_all(&0u32.to_le_bytes()).expect("terminator");
        });
        let mut reader = BufReader::new(client);
        let mut output = FlushSignalWriter {
            bytes: Vec::new(),
            flushed: Some(flushed_tx),
        };
        stream_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            1024,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .expect("stream complete query");
        sender.join().expect("daemon thread");
        assert_eq!(output.bytes.len(), 4 + b"/first".len());
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_reports_truncation_after_provisional_output() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        write_test_query_result(&mut server, b"/provisional");
        drop(server);
        let mut reader = BufReader::new(client);
        let mut output = Vec::new();

        let result = stream_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            1024,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        );

        assert!(result.is_err());
        assert!(!output.is_empty(), "streaming output is provisional");
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_enforces_byte_budget_including_equality_and_zero() {
        for (budget, should_succeed) in [(4 + b"/exact".len() as u64, true), (0, false)] {
            let (mut server, client) = UnixStream::pair().expect("socket pair");
            write_test_query_result(&mut server, b"/exact");
            server.write_all(&0u32.to_le_bytes()).expect("terminator");
            drop(server);
            let mut reader = BufReader::new(client);
            let mut output = Vec::new();
            let result = stream_strict_binary_results_with_limits(
                &mut reader,
                &mut output,
                None,
                budget,
                Instant::now() + Duration::from_secs(1),
                Duration::from_millis(100),
            );
            assert_eq!(result.is_ok(), should_succeed);
        }
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_applies_limit_and_consumes_daemon_terminator() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        write_test_query_result(&mut server, b"/first");
        write_test_query_result(&mut server, b"/second");
        server.write_all(&0u32.to_le_bytes()).expect("terminator");
        drop(server);
        let mut reader = BufReader::new(client);
        let mut output = Vec::new();

        stream_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            Some(1),
            1024,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .expect("stream limited query");

        let mut expected = Vec::new();
        write_binary_path_record(&mut expected, "/first", true).expect("expected record");
        assert_eq!(output, expected);
        let mut trailing = [0; 1];
        assert_eq!(reader.read(&mut trailing).expect("terminator consumed"), 0);
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_preserves_raw_non_utf8_path_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let raw = PathBuf::from(OsString::from_vec(b"/tmp/stream-\xff".to_vec()));
        let encoded = fsx::encode_lossless_path(&raw);
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        write_test_query_result(&mut server, encoded.as_bytes());
        server.write_all(&0u32.to_le_bytes()).expect("terminator");
        drop(server);
        let mut reader = BufReader::new(client);
        let mut output = Vec::new();

        stream_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            1024,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .expect("stream raw path");

        let mut expected = Vec::new();
        write_binary_path_record(&mut expected, &encoded, true).expect("expected raw record");
        assert_eq!(output, expected);
        assert_eq!(&output[4..], raw.as_os_str().as_bytes());
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_obeys_injected_overall_deadline() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        let sender = std::thread::spawn(move || {
            write_test_query_result(&mut server, b"/visible");
            for byte in 0u32.to_le_bytes() {
                if server.write_all(&[byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        let mut reader = BufReader::new(client);
        let mut output = Vec::new();
        let started = Instant::now();

        let result = stream_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            1024,
            started + Duration::from_millis(25),
            Duration::from_secs(1),
        );

        assert!(result.unwrap_err().contains("timed out"));
        assert!(!output.is_empty(), "streamed data remains provisional");
        assert!(started.elapsed() < Duration::from_millis(500));
        sender.join().expect("sender thread");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn strict_stream_stdout_pipe_write_respects_deadline_and_restores_flags() {
        use std::os::fd::{AsRawFd, FromRawFd};

        let mut pipe_fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        let reader_pipe = unsafe { std::fs::File::from_raw_fd(pipe_fds[0]) };
        let writer_pipe = unsafe { std::fs::File::from_raw_fd(pipe_fds[1]) };
        let fd = writer_pipe.as_raw_fd();
        let original_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        assert!(original_flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFL, original_flags | libc::O_NONBLOCK) },
            0
        );
        let full = [0u8; 8192];
        loop {
            let written = unsafe { libc::write(fd, full.as_ptr().cast(), full.len()) };
            if written < 0 {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN)
                );
                break;
            }
        }
        assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETFL, original_flags) }, 0);

        let (mut server, client) = UnixStream::pair().expect("socket pair");
        write_test_query_result(&mut server, b"/blocked");
        server.write_all(&0u32.to_le_bytes()).expect("terminator");
        let mut reader = BufReader::new(client);
        let deadline = Instant::now() + Duration::from_millis(30);
        let started = Instant::now();
        let result = {
            let mut output = DeadlineFdWriter::new(fd, deadline).expect("deadline writer");
            let result = stream_strict_binary_results_with_limits(
                &mut reader,
                &mut output,
                None,
                1024,
                deadline,
                Duration::from_millis(100),
            );
            drop(output);
            result
        };

        assert!(result.unwrap_err().contains("timed out"));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFL) }, original_flags);
        drop(reader_pipe); // Keep the read end alive until after the timed write.
    }

    #[cfg(unix)]
    #[test]
    fn strict_stream_rejects_a_deadline_exceeded_during_final_flush() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        server
            .write_all(&0u32.to_le_bytes())
            .expect("empty terminator");
        let mut reader = BufReader::new(client);
        let deadline = Instant::now() + Duration::from_millis(10);
        let mut output = DelayedFlushWriter {
            delay: Duration::from_millis(30),
            bytes: Vec::new(),
        };

        let result = stream_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            1024,
            deadline,
            Duration::from_millis(100),
        );

        assert!(result.unwrap_err().contains("timed out"));
    }

    #[cfg(unix)]
    #[test]
    fn strict_staging_enforces_an_overall_deadline() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        let sender = std::thread::spawn(move || {
            for byte in 0u32.to_le_bytes() {
                if server.write_all(&[byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        let mut reader = BufReader::new(client);
        let mut output = Vec::new();
        let started = Instant::now();

        let result = stage_strict_binary_results_with_limits(
            &mut reader,
            &mut output,
            None,
            1024,
            started + Duration::from_millis(25),
            Duration::from_secs(1),
        );

        assert!(result.unwrap_err().contains("timed out"));
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(output.is_empty());
        sender.join().expect("sender thread");
    }

    #[cfg(unix)]
    #[test]
    fn strict_spool_is_anonymous_and_released_on_drop() {
        let directory = tempfile::tempdir().expect("temporary directory");
        {
            let spool = StrictBinarySpool::create_in(directory.path()).expect("anonymous spool");
            assert!(directory
                .path()
                .read_dir()
                .expect("list directory")
                .next()
                .is_none());
            assert!(spool
                .writer
                .get_ref()
                .metadata()
                .expect("open file metadata")
                .is_file());
        }
        assert!(directory
            .path()
            .read_dir()
            .expect("list directory")
            .next()
            .is_none());
    }

    #[cfg(unix)]
    fn write_test_query_result(writer: &mut UnixStream, path: &[u8]) {
        let mut frame = vec![0];
        frame.extend_from_slice(&i64::MIN.to_le_bytes());
        frame.extend_from_slice(&i64::MIN.to_le_bytes());
        frame.extend_from_slice(path);
        writer
            .write_all(&(frame.len() as u32).to_le_bytes())
            .and_then(|_| writer.write_all(&frame))
            .expect("write query result");
    }

    #[cfg(unix)]
    #[test]
    fn strict_binary_results_do_not_publish_truncated_streams() {
        let (mut server, client) = UnixStream::pair().expect("socket pair");
        let path = b"/root/complete";
        let mut frame = vec![0];
        frame.extend_from_slice(&i64::MIN.to_le_bytes());
        frame.extend_from_slice(&i64::MIN.to_le_bytes());
        frame.extend_from_slice(path);
        server
            .write_all(&(frame.len() as u32).to_le_bytes())
            .and_then(|_| server.write_all(&frame))
            .and_then(|_| server.write_all(&20u32.to_le_bytes()))
            .and_then(|_| server.write_all(b"short"))
            .expect("write partial response");
        drop(server);

        let mut reader = BufReader::new(client);
        let mut output = Vec::new();
        assert!(stage_strict_binary_results(
            &mut reader,
            &mut output,
            None,
            Duration::from_millis(100),
            Instant::now() + Duration::from_secs(1),
        )
        .is_err());
        assert!(output.is_empty(), "partial candidates escaped staging");
    }

    #[cfg(unix)]
    struct FlushSignalWriter {
        bytes: Vec<u8>,
        flushed: Option<std::sync::mpsc::Sender<Vec<u8>>>,
    }

    #[cfg(unix)]
    impl Write for FlushSignalWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if let Some(flushed) = self.flushed.take() {
                flushed
                    .send(self.bytes.clone())
                    .map_err(|error| io::Error::new(io::ErrorKind::BrokenPipe, error))?;
            }
            Ok(())
        }
    }

    #[cfg(unix)]
    struct DelayedFlushWriter {
        delay: Duration,
        bytes: Vec<u8>,
    }

    #[cfg(unix)]
    impl Write for DelayedFlushWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            std::thread::sleep(self.delay);
            Ok(())
        }
    }

    #[cfg(unix)]
    #[test]
    fn binary_index_record_restores_non_utf8_path_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let raw = PathBuf::from(OsString::from_vec(b"/tmp/invalid-\xff".to_vec()));
        let encoded = fsx::encode_lossless_path(&raw);
        let mut output = Vec::new();
        write_binary_path_record(&mut output, &encoded, true).expect("write record");

        let length = u32::from_le_bytes(output[..4].try_into().expect("length")) as usize;
        assert_eq!(length, raw.as_os_str().as_bytes().len());
        assert_eq!(&output[4..], raw.as_os_str().as_bytes());
    }
}
