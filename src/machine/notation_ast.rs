//! Position-preserving AST for machine notation language intelligence.
//!
//! Wraps the existing pest parser to capture token positions for:
//! - Semantic syntax coloring
//! - Completion context detection
//! - Hover information
//! - Diagnostic ranges
//!
//! Unlike `parse_machine()` which discards position information and fails
//! on the first error, this module always returns a partial AST — even on
//! parse failures — so that language tooling works during active editing.

use std::collections::{BTreeMap, HashMap};

use pest::Parser;
use uuid::Uuid;

use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;

use super::error::MachineSyntaxError;
use super::parser::{MachineParser, Rule};

// =============================================================================
// Position types
// =============================================================================

/// 0-based line and character position in notation text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotationPosition {
    pub line: usize,
    pub character: usize,
}

/// Span in the source text with both line/character and byte offsets.
#[derive(Debug, Clone)]
pub struct NotationSpan {
    pub start: NotationPosition,
    pub end: NotationPosition,
    pub start_byte: usize,
    pub end_byte: usize,
}

impl NotationSpan {
    /// Whether a 0-based (line, character) position falls within this span.
    pub fn contains(&self, line: usize, character: usize) -> bool {
        let pos = NotationPosition { line, character };
        if pos.line < self.start.line || pos.line > self.end.line {
            return false;
        }
        if pos.line == self.start.line && pos.character < self.start.character {
            return false;
        }
        if pos.line == self.end.line && pos.character >= self.end.character {
            return false;
        }
        true
    }
}

// =============================================================================
// Parsed statements
// =============================================================================

/// A parsed header statement with full position information.
#[derive(Debug, Clone)]
pub struct ParsedHeader {
    pub alias: String,
    pub alias_span: NotationSpan,
    pub cap_urn_str: String,
    pub cap_urn_span: NotationSpan,
    /// Parsed CapUrn — None if cap URN string failed to parse.
    pub cap_urn: Option<CapUrn>,
    pub statement_span: NotationSpan,
}

/// A parsed wiring statement with full position information.
#[derive(Debug, Clone)]
pub struct ParsedWiring {
    pub sources: Vec<(String, NotationSpan)>,
    pub cap_alias: String,
    pub cap_alias_span: NotationSpan,
    pub target: String,
    pub target_span: NotationSpan,
    pub arrow_spans: Vec<NotationSpan>,
    pub statement_span: NotationSpan,
    /// Open and close paren spans for fan-in groups.
    pub paren_spans: Option<(NotationSpan, NotationSpan)>,
    pub comma_spans: Vec<NotationSpan>,
}

/// A parsed statement — either a header or a wiring.
#[derive(Debug, Clone)]
pub enum ParsedStatement {
    Header(ParsedHeader),
    Wiring(ParsedWiring),
}

// =============================================================================
// Notation AST
// =============================================================================

/// Full AST result from parsing machine notation with position information.
///
/// Always contains as much information as could be extracted — even on parse
/// failures. The `error` field holds the first error encountered (if any).
///
/// `NotationAST` is a purely lexical / syntactic view: the editor consumes
/// it for highlighting, hover, and completion. It does NOT carry a resolved
/// `Machine`, since resolution requires the cap registry (out of scope for
/// the lexical analyzer).
#[derive(Debug)]
pub struct NotationAST {
    pub statements: Vec<ParsedStatement>,
    /// (open bracket, close bracket) spans for each statement.
    pub bracket_spans: Vec<(NotationSpan, NotationSpan)>,
    /// Alias → parsed header mapping (for lookup by name).
    pub alias_map: BTreeMap<String, ParsedHeader>,
    /// Node name → derived media URN mapping.
    pub node_media: HashMap<String, MediaUrn>,
    /// First error encountered during parsing, if any.
    pub error: Option<MachineSyntaxError>,
}

// =============================================================================
// Semantic token types (for coloring)
// =============================================================================

/// Token type for semantic coloring. Maps 1:1 to the proto SemanticTokenType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticTokenType {
    Bracket,
    Paren,
    Comma,
    Semicolon,
    CapPrefix,
    MediaPrefix,
    KeywordIn,
    KeywordOut,
    KeywordOp,
    Assignment,
    Arrow,
    Alias,
    Node,
    String,
    Escape,
    TagKey,
    TagValue,
}

/// A semantic token with position and type.
#[derive(Debug, Clone)]
pub struct SemanticTokenInfo {
    pub line: usize,
    pub start_character: usize,
    pub length: usize,
    pub token_type: SemanticTokenType,
}

// =============================================================================
// Editor model
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotationEntityKind {
    AliasDefinition,
    CapUrn,
    Node,
    CapAliasReference,
    Arrow,
}

#[derive(Debug, Clone)]
pub struct NotationEntityInfo {
    /// Stable UUID for the logical token this entity represents. Entities
    /// that share a `token_id` are all source-span occurrences of the same
    /// underlying concept and should be highlighted together.
    pub token_id: String,
    pub kind: NotationEntityKind,
    pub range: NotationSpan,
    pub label: String,
    pub detail: Option<String>,
    pub hover_markdown: Option<String>,
    pub linked_cap_urn: Option<String>,
    pub color_index: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotationGraphElementKind {
    Node,
    Cap,
    Edge,
}

#[derive(Debug, Clone)]
pub struct NotationGraphElementInfo {
    /// Stable UUID for the logical token this graph element represents.
    /// Shared with all `NotationEntityInfo` records that represent the
    /// same token in the source text.
    pub token_id: String,
    /// Graph-local identifier used by Cytoscape for layout and edge
    /// wiring. Distinct from `token_id` so edges can reference their
    /// endpoints directly without having to resolve token identity.
    pub graph_id: String,
    pub kind: NotationGraphElementKind,
    pub label: String,
    pub detail: Option<String>,
    pub linked_cap_urn: Option<String>,
    pub linked_media_urn: Option<String>,
    pub is_sequence: Option<bool>,
    pub source_graph_id: Option<String>,
    pub target_graph_id: Option<String>,
    pub color_index: Option<i32>,
    pub is_loop: bool,
}

// =============================================================================
// Completion context
// =============================================================================

/// Completion context at a cursor position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionContextType {
    /// After `[`, before any `cap:` — suggests aliases, ops.
    HeaderStart,
    /// After `cap:` — suggests full cap URNs.
    CapUrn,
    /// After `in=` or `out=` inside a cap URN — suggests media URNs.
    MediaUrn,
    /// In wiring source position (before first `->` or between arrows).
    WiringSource,
    /// In wiring target position (after the second `->`).
    WiringTarget,
    /// No valid completion context.
    Unknown,
}

// =============================================================================
// Core parsing function
// =============================================================================

/// Parse machine notation into a position-preserving AST.
///
/// Unlike `parse_machine()`, this function:
/// - Never panics or returns Err — always returns an AST
/// - Preserves all token positions via pest's span API
/// - Returns partial results on parse failure
/// - Attempts semantic resolution (cap URNs, media URNs) but records
///   errors rather than failing
///
/// This is the primary entry point for all language intelligence features.
pub fn parse_notation_ast(input: &str) -> NotationAST {
    let mut ast = NotationAST {
        statements: Vec::new(),
        bracket_spans: Vec::new(),
        alias_map: BTreeMap::new(),
        node_media: HashMap::new(),
        error: None,
    };

    let trimmed = input.trim();
    if trimmed.is_empty() {
        ast.error = Some(MachineSyntaxError::Empty);
        return ast;
    }

    // Phase 1: PEG parse
    let pairs = match MachineParser::parse(Rule::program, trimmed) {
        Ok(pairs) => pairs,
        Err(e) => {
            ast.error = Some(MachineSyntaxError::ParseError {
                details: format!("{}", e),
            });
            // On parse failure, do a best-effort bracket scan for partial tokens
            scan_brackets_for_partial_ast(trimmed, &mut ast);
            return ast;
        }
    };

    // Phase 2: Walk the pest AST and collect statements with positions.
    // We need the byte offset of the trimmed string relative to the original
    // input, so that positions are correct for the original text.
    let trim_offset = input.len() - input.trim_end().len();
    let trim_start = input.len() - trimmed.len() - trim_offset;

    let program = match pairs.into_iter().next() {
        Some(p) => p,
        None => return ast,
    };

    for pair in program.into_inner() {
        if pair.as_rule() != Rule::stmt {
            continue; // skip EOI
        }

        // Bracket spans: for bracketed statements `[inner]`, extract
        // bracket positions. Line-based statements have no brackets.
        let stmt_span = pest_span_to_notation_span(&pair.as_span(), trimmed, trim_start);
        let is_bracketed = pair.as_str().starts_with('[');

        if is_bracketed {
            let open_bracket = NotationSpan {
                start: stmt_span.start,
                end: NotationPosition {
                    line: stmt_span.start.line,
                    character: stmt_span.start.character + 1,
                },
                start_byte: stmt_span.start_byte,
                end_byte: stmt_span.start_byte + 1,
            };
            let close_bracket = NotationSpan {
                start: NotationPosition {
                    line: stmt_span.end.line,
                    character: stmt_span.end.character.saturating_sub(1),
                },
                end: stmt_span.end,
                start_byte: stmt_span.end_byte.saturating_sub(1),
                end_byte: stmt_span.end_byte,
            };
            ast.bracket_spans.push((open_bracket, close_bracket));
        }

        // Descend into inner → header | wiring
        let inner = match pair.into_inner().next() {
            Some(p) => p,
            None => continue,
        };
        let content = match inner.into_inner().next() {
            Some(p) => p,
            None => continue,
        };

        match content.as_rule() {
            Rule::header => {
                let mut inner_pairs = content.into_inner();

                let alias_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let alias = alias_pair.as_str().to_string();
                let alias_span =
                    pest_span_to_notation_span(&alias_pair.as_span(), trimmed, trim_start);

                let cap_urn_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let cap_urn_str = cap_urn_pair.as_str().to_string();
                let cap_urn_span =
                    pest_span_to_notation_span(&cap_urn_pair.as_span(), trimmed, trim_start);

                let cap_urn = CapUrn::from_string(&cap_urn_str).ok();

                let header = ParsedHeader {
                    alias: alias.clone(),
                    alias_span,
                    cap_urn_str,
                    cap_urn_span,
                    cap_urn,
                    statement_span: stmt_span,
                };

                ast.alias_map.insert(alias, header.clone());
                ast.statements.push(ParsedStatement::Header(header));
            }
            Rule::wiring => {
                let mut inner_pairs = content.into_inner();

                // Parse source (single alias or group)
                let source_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let (sources, paren_spans, comma_spans) =
                    parse_source_with_spans(&source_pair, trimmed, trim_start);

                // First arrow
                let arrow1 = inner_pairs.next();
                let mut arrow_spans = Vec::new();
                if let Some(ref a) = arrow1 {
                    arrow_spans.push(pest_span_to_notation_span(
                        &a.as_span(),
                        trimmed,
                        trim_start,
                    ));
                }

                // Cap alias (the cap-position node name)
                let cap_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let cap_alias = cap_pair.as_str().to_string();
                let cap_alias_span =
                    pest_span_to_notation_span(&cap_pair.as_span(), trimmed, trim_start);

                // Second arrow
                let arrow2 = inner_pairs.next();
                if let Some(ref a) = arrow2 {
                    arrow_spans.push(pest_span_to_notation_span(
                        &a.as_span(),
                        trimmed,
                        trim_start,
                    ));
                }

                // Target alias
                let target_pair = match inner_pairs.next() {
                    Some(p) => p,
                    None => continue,
                };
                let target = target_pair.as_str().to_string();
                let target_span =
                    pest_span_to_notation_span(&target_pair.as_span(), trimmed, trim_start);

                let wiring = ParsedWiring {
                    sources,
                    cap_alias,
                    cap_alias_span,
                    target,
                    target_span,
                    arrow_spans,
                    statement_span: stmt_span,
                    paren_spans,
                    comma_spans,
                };

                ast.statements.push(ParsedStatement::Wiring(wiring));
            }
            _ => {}
        }
    }

    // Phase 3: Build alias map and resolve node media types.
    // Check for duplicate aliases.
    let mut seen_aliases: BTreeMap<String, usize> = BTreeMap::new();
    for (idx, stmt) in ast.statements.iter().enumerate() {
        if let ParsedStatement::Header(h) = stmt {
            if let Some(first_idx) = seen_aliases.get(&h.alias) {
                if ast.error.is_none() {
                    ast.error = Some(MachineSyntaxError::DuplicateAlias {
                        alias: h.alias.clone(),
                        first_position: *first_idx,
                    });
                }
            } else {
                seen_aliases.insert(h.alias.clone(), idx);
            }
        }
    }

    // Phase 4: Build the lexical node_media map from the AST wirings + alias map.
    // This is the editor's lexical view; full anchor resolution against the cap
    // registry is done elsewhere (the gRPC layer calls `Machine::from_string` for the
    // resolved view). Node sequence-shape (cardinality) is NOT lexical — it needs cap
    // definitions — so it is derived in `build_editor_model` from `cap_shapes`.
    build_node_media(&ast.statements, &ast.alias_map, &mut ast.node_media);

    ast
}

/// Build node_media mapping from wiring statements and the alias map.
fn build_node_media(
    statements: &[ParsedStatement],
    alias_map: &BTreeMap<String, ParsedHeader>,
    node_media: &mut HashMap<String, MediaUrn>,
) {
    for stmt in statements {
        if let ParsedStatement::Wiring(w) = stmt {
            // Look up the cap URN for this wiring's alias
            let cap_urn = match alias_map.get(&w.cap_alias) {
                Some(h) => match &h.cap_urn {
                    Some(u) => u,
                    None => continue,
                },
                None => continue,
            };

            // Derive media URNs from cap's in=/out= specs
            if let Ok(in_media) = cap_urn.in_media_urn() {
                if let Some((name, _)) = w.sources.first() {
                    if !alias_map.contains_key(name) && !node_media.contains_key(name) {
                        node_media.insert(name.clone(), in_media);
                    }
                }
            }
            if let Ok(out_media) = cap_urn.out_media_urn() {
                if !alias_map.contains_key(&w.target) && !node_media.contains_key(&w.target) {
                    node_media.insert(w.target.clone(), out_media);
                }
            }
        }
    }
}

// =============================================================================
// Completion context detection
// =============================================================================

/// Determine the completion context at a given cursor position.
///
/// This is a pure text-scanning function — it does not need a parsed AST.
/// It follows the same algorithm as the TypeScript LSP `getContext()`:
///
/// 1. Convert (line, character) to a byte offset in the text
/// 2. Find the innermost unclosed `[` before the cursor
/// 3. Examine the text between `[` and cursor to determine context
///
/// Returns the context type and the prefix string typed so far.
pub fn get_completion_context(
    text: &str,
    line: usize,
    character: usize,
) -> (CompletionContextType, String) {
    // Convert (line, character) to byte offset
    let offset = match line_char_to_offset(text, line, character) {
        Some(o) => o,
        None => return (CompletionContextType::Unknown, String::new()),
    };

    // Find the start of the current statement context: either the
    // innermost unclosed `[` (bracketed mode) or the start of the
    // current line (line-based mode).
    let (context_start, skip, open_bracket) = match find_innermost_open_bracket(text, offset) {
        Some(pos) => (pos, 1, Some(pos)), // skip the `[`
        None => {
            // Line-based mode: find start of current line
            let line_start = text[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
            (line_start, 0, None)
        }
    };
    let statement_end = find_statement_end(text, context_start, skip, open_bracket);
    let statement_body = &text[context_start + skip..statement_end];
    let before_cursor = &text[context_start + skip..offset];

    // Check if we're inside a cap URN (contains "cap:" prefix)
    if let Some(cap_pos) = before_cursor.find("cap:") {
        let after_cap = &before_cursor[cap_pos..];

        // Check if cursor is after `in=` or `out=` — media URN context
        // Look for the last `in=` or `out=` before cursor
        let last_in_eq = after_cap.rfind("in=");
        let last_out_eq = after_cap.rfind("out=");
        let last_semicolon = after_cap.rfind(';');

        let media_eq_pos = match (last_in_eq, last_out_eq) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        if let Some(eq_pos) = media_eq_pos {
            // Is the cursor after this in=/out= and not past a subsequent semicolon?
            let eq_end = if after_cap[eq_pos..].starts_with("in=") {
                eq_pos + 3
            } else {
                eq_pos + 4
            };

            let past_semicolon = match last_semicolon {
                Some(sc) => sc > eq_pos,
                None => false,
            };

            if !past_semicolon {
                // Extract prefix after the `=` (handle quoted values)
                let after_eq = &after_cap[eq_end..];
                let prefix = extract_media_prefix(after_eq);
                return (CompletionContextType::MediaUrn, prefix);
            }
        }

        // Not in a media URN position — we're completing the cap URN itself
        let prefix = after_cap.to_string();
        return (CompletionContextType::CapUrn, prefix);
    }

    // Check if the current statement is a wiring, even if the cursor is before
    // the first arrow in an existing line. This is what allows manual
    // completion to surface document-local node and alias references anywhere
    // inside a wiring statement.
    if statement_body.contains("->") {
        let prefix = extract_word_prefix(before_cursor);
        let arrows_before_cursor = before_cursor.matches("->").count();
        if arrows_before_cursor == 0 {
            return (CompletionContextType::WiringSource, prefix);
        }
        return (CompletionContextType::WiringTarget, prefix);
    }

    // Default: identifier/header start — line-based notation is valid without
    // brackets, so a plain identifier position is a completion site.
    let prefix = extract_word_prefix(before_cursor);
    (CompletionContextType::HeaderStart, prefix)
}

/// Compute the full token replacement span for completions at a cursor
/// position. The returned span covers the token under the caret, not just the
/// left-side prefix, so the UI can replace an in-progress identifier without
/// reconstructing token boundaries locally.
pub fn get_completion_replacement_span(
    text: &str,
    line: usize,
    character: usize,
) -> Option<NotationSpan> {
    let cursor_offset = line_char_to_offset(text, line, character)?;
    let chars: Vec<(usize, char)> = text.char_indices().collect();

    let mut start_byte = cursor_offset;
    let mut end_byte = cursor_offset;

    for (byte_idx, ch) in chars.iter().rev() {
        if *byte_idx >= cursor_offset {
            continue;
        }
        if is_completion_boundary_char(*ch) {
            break;
        }
        start_byte = *byte_idx;
    }

    for (byte_idx, ch) in chars.iter() {
        if *byte_idx < cursor_offset {
            continue;
        }
        if is_completion_boundary_char(*ch) {
            end_byte = *byte_idx;
            break;
        }
        end_byte = *byte_idx + ch.len_utf8();
    }

    Some(NotationSpan {
        start: byte_offset_to_position(text, start_byte),
        end: byte_offset_to_position(text, end_byte),
        start_byte,
        end_byte,
    })
}

// =============================================================================
// Hover information
// =============================================================================

/// Generate hover information for the token at the given position.
///
/// Searches the AST for a token spanning the cursor position and generates
/// markdown hover content.
///
/// `cap_details` and `media_details` provide registry enrichment data:
/// - cap_details: cap URN string → (title, description)
/// - media_details: media URN string → (title, description)
///
/// Returns (markdown_content, token_span) or None if no hoverable token.
/// Build a digested editor model from the parsed AST.
///
/// Every logical token (node, cap invocation, alias definition, cap URN,
/// arrow) gets a stable UUID `token_id`. That same id is
/// stamped on every `NotationEntityInfo` record that anchors the token in
/// the source text, AND on every `NotationGraphElementInfo` record that
/// represents the token in the graph preview. The UI can then cross-
/// highlight in either direction by a trivial `token_id == token_id`
/// lookup — no walks over `graph_element_ids` / `linked_entity_ids`
/// arrays, no reconstruction of identity from labels or positions.
///
/// Identity rules:
///   - A node name is one logical token, even if it appears in multiple
///     wirings. All text entities for that name share one `token_id`, and
///     the single graph node carries the same id.
///   - A cap invocation (the `alias` in `foo -> alias -> bar`) is one
///     logical token per wiring statement. The cap's `token_id` is shared
///     by: the cap alias reference text entity, any arrows in that wiring,
///     and the graph's cap element.
///   - Each alias definition in a header and each cap URN in a header is
///     its own logical token with no corresponding graph element.
///   - Each wiring edge is its own logical token. The arrows flanking the
///     cap alias share that wiring's cap token_id (they are interaction
///     surfaces on the cap) rather than being separate edge tokens —
///     this is what the user intuitively expects when hovering an arrow.
pub fn build_editor_model(
    ast: &NotationAST,
    cap_details: &HashMap<String, (String, String, Option<String>)>,
    media_details: &HashMap<String, (String, String, Option<String>)>,
    cap_shapes: &HashMap<String, (bool, bool)>,
) -> (Vec<NotationEntityInfo>, Vec<NotationGraphElementInfo>) {
    let mut entities = Vec::new();
    let mut graph_elements = Vec::new();
    // Node name → (token_id, graph_id). Ensures the same node name
    // always resolves to one logical token and one graph node.
    let mut node_identity_by_name: HashMap<String, (String, String)> = HashMap::new();
    let mut wiring_index = 0usize;

    // Cardinality of every node, derived from cap shapes (`cap_shapes`: cap URN →
    // `(input_is_sequence, output_is_sequence)`). A node holds a sequence iff the cap
    // that PRODUCES it (the wiring whose target is that node) has a sequence output;
    // nodes that are never a target are scalar roots. This is exactly the rule
    // `resolve::resolve_pre_interned` and `live_cap_fab::get_outgoing_edges` use — the
    // single source of truth — so the editor's ForEach badge matches execution. It
    // replaces the retired `LOOP` keyword: ForEach is shown wherever a sequence feeds
    // a scalar-input cap, never authored.
    let mut node_is_sequence: HashMap<String, bool> = HashMap::new();
    for stmt in &ast.statements {
        if let ParsedStatement::Wiring(wiring) = stmt {
            let output_is_sequence = ast
                .alias_map
                .get(&wiring.cap_alias)
                .and_then(|header| cap_shapes.get(&header.cap_urn_str))
                .map(|(_, out)| *out)
                .unwrap_or(false);
            node_is_sequence.insert(wiring.target.clone(), output_is_sequence);
        }
    }

    for stmt in &ast.statements {
        match stmt {
            ParsedStatement::Header(header) => {
                entities.push(NotationEntityInfo {
                    token_id: new_token_id(),
                    kind: NotationEntityKind::AliasDefinition,
                    range: header.alias_span.clone(),
                    label: header.alias.clone(),
                    detail: Some("Capability alias definition".to_string()),
                    hover_markdown: Some(format_alias_hover(header, cap_details)),
                    linked_cap_urn: Some(header.cap_urn_str.clone()),
                    color_index: None,
                });
                entities.push(NotationEntityInfo {
                    token_id: new_token_id(),
                    kind: NotationEntityKind::CapUrn,
                    range: header.cap_urn_span.clone(),
                    label: header.cap_urn_str.clone(),
                    detail: cap_details
                        .get(&header.cap_urn_str)
                        .map(|(title, _, _)| title.clone()),
                    hover_markdown: Some(format_cap_urn_hover(
                        &header.cap_urn_str,
                        &header.cap_urn,
                        cap_details,
                    )),
                    linked_cap_urn: Some(header.cap_urn_str.clone()),
                    color_index: None,
                });
            }
            ParsedStatement::Wiring(wiring) => {
                let color_index = wiring_index as i32;
                let cap_token_id = new_token_id();
                let cap_fab_id = format!("cap-{wiring_index}");

                let linked_cap_urn = ast
                    .alias_map
                    .get(&wiring.cap_alias)
                    .map(|header| header.cap_urn_str.clone());

                // Cap alias reference in the wiring body (the `alias` in
                // `src -> alias -> dst`).
                entities.push(NotationEntityInfo {
                    token_id: cap_token_id.clone(),
                    kind: NotationEntityKind::CapAliasReference,
                    range: wiring.cap_alias_span.clone(),
                    label: wiring.cap_alias.clone(),
                    detail: linked_cap_urn.clone(),
                    hover_markdown: ast
                        .alias_map
                        .get(&wiring.cap_alias)
                        .map(|header| format_alias_hover(header, cap_details)),
                    linked_cap_urn: linked_cap_urn.clone(),
                    color_index: Some(color_index),
                });

                // Whether this cap maps per-item (ForEach), derived from cardinality:
                // its primary source node carries a sequence but the cap consumes a
                // scalar. `Cap::needs_foreach` is the canonical rule; here we apply it
                // to the editor's cap shapes so the badge matches what execution does.
                let (input_is_sequence, _output_is_sequence) = linked_cap_urn
                    .as_deref()
                    .and_then(|urn| cap_shapes.get(urn))
                    .copied()
                    .unwrap_or((false, false));
                let primary_source_is_sequence = wiring
                    .sources
                    .first()
                    .map(|(name, _)| node_is_sequence.get(name).copied().unwrap_or(false))
                    .unwrap_or(false);
                let is_loop = primary_source_is_sequence && !input_is_sequence;

                // Source nodes: each named node resolves to ONE logical
                // token and ONE graph node, shared across all wirings.
                let mut source_identities: Vec<(String, String)> = Vec::new();
                for (source_name, source_span) in &wiring.sources {
                    let (source_token_id, source_graph_id) = ensure_graph_node(
                        &mut graph_elements,
                        &mut node_identity_by_name,
                        source_name,
                        ast,
                        media_details,
                        &node_is_sequence,
                    );
                    source_identities.push((source_token_id.clone(), source_graph_id));

                    entities.push(NotationEntityInfo {
                        token_id: source_token_id,
                        kind: NotationEntityKind::Node,
                        range: source_span.clone(),
                        label: source_name.clone(),
                        detail: ast.node_media.get(source_name).map(|urn| urn.to_string()),
                        hover_markdown: Some(format_node_hover(
                            source_name,
                            &ast.node_media,
                            media_details,
                        )),
                        linked_cap_urn: None,
                        color_index: None,
                    });
                }

                // Target node.
                let (target_token_id, target_graph_id) = ensure_graph_node(
                    &mut graph_elements,
                    &mut node_identity_by_name,
                    &wiring.target,
                    ast,
                    media_details,
                    &node_is_sequence,
                );
                entities.push(NotationEntityInfo {
                    token_id: target_token_id.clone(),
                    kind: NotationEntityKind::Node,
                    range: wiring.target_span.clone(),
                    label: wiring.target.clone(),
                    detail: ast
                        .node_media
                        .get(&wiring.target)
                        .map(|urn| urn.to_string()),
                    hover_markdown: Some(format_node_hover(
                        &wiring.target,
                        &ast.node_media,
                        media_details,
                    )),
                    linked_cap_urn: None,
                    color_index: None,
                });

                // Arrows in this wiring share the cap's token_id. Hovering
                // an arrow highlights the cap and its adjacent nodes.
                for arrow_span in &wiring.arrow_spans {
                    entities.push(NotationEntityInfo {
                        token_id: cap_token_id.clone(),
                        kind: NotationEntityKind::Arrow,
                        range: arrow_span.clone(),
                        label: "->".to_string(),
                        detail: Some("Wiring edge".to_string()),
                        hover_markdown: None,
                        linked_cap_urn: linked_cap_urn.clone(),
                        color_index: Some(color_index),
                    });
                }

                // Graph edges: one per source connection + one output
                // edge. They share the cap's token_id so hovering the
                // cap in text lights up all its edges in the graph.
                for (source_idx, (_, source_graph_id)) in source_identities.iter().enumerate() {
                    let edge_label = if source_identities.len() == 1 {
                        "in".to_string()
                    } else {
                        format!("in {}", source_idx + 1)
                    };
                    graph_elements.push(NotationGraphElementInfo {
                        token_id: cap_token_id.clone(),
                        graph_id: format!("edge-{wiring_index}-in-{source_idx}"),
                        kind: NotationGraphElementKind::Edge,
                        label: edge_label,
                        detail: Some("Source connection".to_string()),
                        linked_cap_urn: linked_cap_urn.clone(),
                        linked_media_urn: None,
                        is_sequence: Some(
                            wiring
                                .sources
                                .get(source_idx)
                                .map(|(name, _)| {
                                    node_is_sequence.get(name).copied().unwrap_or(false)
                                })
                                .unwrap_or(false),
                        ),
                        source_graph_id: Some(source_graph_id.clone()),
                        target_graph_id: Some(cap_fab_id.clone()),
                        color_index: Some(color_index),
                        is_loop,
                    });
                }

                graph_elements.push(NotationGraphElementInfo {
                    token_id: cap_token_id.clone(),
                    graph_id: format!("edge-{wiring_index}-out"),
                    kind: NotationGraphElementKind::Edge,
                    label: "out".to_string(),
                    detail: Some("Result connection".to_string()),
                    linked_cap_urn: linked_cap_urn.clone(),
                    linked_media_urn: None,
                    is_sequence: Some(
                        node_is_sequence
                            .get(&wiring.target)
                            .copied()
                            .unwrap_or(false),
                    ),
                    source_graph_id: Some(cap_fab_id.clone()),
                    target_graph_id: Some(target_graph_id),
                    color_index: Some(color_index),
                    is_loop,
                });

                graph_elements.push(NotationGraphElementInfo {
                    token_id: cap_token_id,
                    graph_id: cap_fab_id,
                    kind: NotationGraphElementKind::Cap,
                    label: wiring.cap_alias.clone(),
                    detail: linked_cap_urn.clone(),
                    linked_cap_urn,
                    linked_media_urn: None,
                    is_sequence: Some(is_loop),
                    source_graph_id: None,
                    target_graph_id: None,
                    color_index: Some(color_index),
                    is_loop,
                });

                wiring_index += 1;
            }
        }
    }

    (entities, graph_elements)
}

fn new_token_id() -> String {
    Uuid::new_v4().to_string()
}

// =============================================================================
// Semantic token emission
// =============================================================================

/// Emit all semantic tokens for syntax coloring.
///
/// Walks the AST and emits tokens for every syntactic element:
/// - Brackets `[]`, parens `()`, commas, semicolons
/// - Cap URN internals (cap:, in, out, op, =, ;, quoted strings, media:, tags)
/// - Arrows `->`
/// - Aliases and node names
pub fn emit_semantic_tokens(ast: &NotationAST, _input: &str) -> Vec<SemanticTokenInfo> {
    let mut tokens = Vec::new();

    // Bracket spans
    for (open, close) in &ast.bracket_spans {
        tokens.push(span_to_token(open, SemanticTokenType::Bracket));
        tokens.push(span_to_token(close, SemanticTokenType::Bracket));
    }

    for stmt in &ast.statements {
        match stmt {
            ParsedStatement::Header(h) => {
                // Alias in header
                tokens.push(span_to_token(&h.alias_span, SemanticTokenType::Alias));

                // Whole cap URN rendered in one color. The URN internals
                // (cap:, in=, media:, op=, etc.) are a single notation-
                // level concept — breaking them up into a rainbow of
                // per-subtoken colors is noise. The `CapPrefix` token
                // type acts as the "cap URN" color.
                tokens.push(span_to_token(&h.cap_urn_span, SemanticTokenType::CapPrefix));
            }
            ParsedStatement::Wiring(w) => {
                // Parens for fan-in groups
                if let Some((open_paren, close_paren)) = &w.paren_spans {
                    tokens.push(span_to_token(open_paren, SemanticTokenType::Paren));
                    tokens.push(span_to_token(close_paren, SemanticTokenType::Paren));
                }

                // Commas in fan-in groups
                for comma_span in &w.comma_spans {
                    tokens.push(span_to_token(comma_span, SemanticTokenType::Comma));
                }

                // Source nodes
                for (_, span) in &w.sources {
                    tokens.push(span_to_token(span, SemanticTokenType::Node));
                }

                // Arrows
                for arrow_span in &w.arrow_spans {
                    tokens.push(span_to_token(arrow_span, SemanticTokenType::Arrow));
                }

                // Cap alias in wiring (reference to a defined capability)
                tokens.push(span_to_token(&w.cap_alias_span, SemanticTokenType::Alias));

                // Target node
                tokens.push(span_to_token(&w.target_span, SemanticTokenType::Node));
            }
        }
    }

    // Sort by position
    tokens.sort_by(|a, b| {
        a.line
            .cmp(&b.line)
            .then(a.start_character.cmp(&b.start_character))
    });

    tokens
}

// =============================================================================
// Helper: Pest span → NotationSpan
// =============================================================================

/// Convert a pest `Span` to a `NotationSpan` with 0-based line/character.
fn pest_span_to_notation_span(
    span: &pest::Span,
    source: &str,
    offset_in_original: usize,
) -> NotationSpan {
    let start_byte = span.start() + offset_in_original;
    let end_byte = span.end() + offset_in_original;

    let start = byte_offset_to_position(source, span.start());
    let end = byte_offset_to_position(source, span.end());

    NotationSpan {
        start,
        end,
        start_byte,
        end_byte,
    }
}

/// Convert a byte offset in `source` to a 0-based (line, character) position.
pub fn byte_offset_to_position(source: &str, byte_offset: usize) -> NotationPosition {
    let mut line = 0;
    let mut col = 0;

    for (i, ch) in source.char_indices() {
        if i >= byte_offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
    }

    NotationPosition {
        line,
        character: col,
    }
}

/// Convert 0-based (line, character) to byte offset. Returns None if out of bounds.
fn line_char_to_offset(text: &str, line: usize, character: usize) -> Option<usize> {
    let mut current_line = 0;
    let mut current_col = 0;

    for (i, ch) in text.char_indices() {
        if current_line == line && current_col == character {
            return Some(i);
        }
        if ch == '\n' {
            if current_line == line {
                // Character is past end of this line — clamp to end
                return Some(i);
            }
            current_line += 1;
            current_col = 0;
        } else {
            current_col += 1;
        }
    }

    // Cursor at very end of text
    if current_line == line && current_col == character {
        return Some(text.len());
    }

    None
}

fn is_completion_boundary_char(ch: char) -> bool {
    matches!(
        ch,
        ' ' | '[' | ']' | ';' | '=' | '>' | '(' | ')' | ',' | '\n' | '\r' | '\t'
    )
}

/// Resolve the `(token_id, graph_id)` pair for a node by name, creating
/// the graph element on first sight. Subsequent calls for the same node
/// name reuse the existing identity so every occurrence of the name in
/// the source text maps to exactly one logical token and one graph node.
fn ensure_graph_node(
    graph_elements: &mut Vec<NotationGraphElementInfo>,
    node_identity_by_name: &mut HashMap<String, (String, String)>,
    node_name: &str,
    ast: &NotationAST,
    media_details: &HashMap<String, (String, String, Option<String>)>,
    node_is_sequence: &HashMap<String, bool>,
) -> (String, String) {
    if let Some(existing) = node_identity_by_name.get(node_name) {
        return existing.clone();
    }

    let token_id = new_token_id();
    let graph_id = format!("node-{node_name}");
    let linked_media_urn = ast.node_media.get(node_name).map(|urn| urn.to_string());
    let detail = linked_media_urn
        .as_ref()
        .and_then(|urn| {
            media_details.get(urn).map(|(title, description, _)| {
                if title.is_empty() {
                    description.clone()
                } else if description.is_empty() {
                    title.clone()
                } else {
                    format!("{title} — {description}")
                }
            })
        })
        .or_else(|| linked_media_urn.clone());

    graph_elements.push(NotationGraphElementInfo {
        token_id: token_id.clone(),
        graph_id: graph_id.clone(),
        kind: NotationGraphElementKind::Node,
        label: node_name.to_string(),
        detail,
        linked_cap_urn: None,
        linked_media_urn,
        is_sequence: Some(node_is_sequence.get(node_name).copied().unwrap_or(false)),
        source_graph_id: None,
        target_graph_id: None,
        color_index: None,
        is_loop: false,
    });
    node_identity_by_name.insert(node_name.to_string(), (token_id.clone(), graph_id.clone()));
    (token_id, graph_id)
}

// =============================================================================
// Helper: Bracket scanning for partial ASTs
// =============================================================================

/// Scan raw text for `[...]` brackets and extract what we can from
/// partial/malformed notation. Used when pest parse fails.
/// Handles both bracketed and line-based statements.
fn scan_brackets_for_partial_ast(text: &str, ast: &mut NotationAST) {
    let mut i = 0;
    let chars: Vec<char> = text.chars().collect();

    while i < chars.len() {
        if chars[i] == '[' {
            let open_pos = byte_offset_to_position(text, i);
            let open_byte = i;

            // Scan for matching `]`, respecting quoted strings
            let mut j = i + 1;
            let mut depth = 0;
            let mut in_quote = false;
            let mut escaped = false;

            while j < chars.len() {
                if escaped {
                    escaped = false;
                    j += 1;
                    continue;
                }
                match chars[j] {
                    '\\' if in_quote => {
                        escaped = true;
                        j += 1;
                    }
                    '"' => {
                        in_quote = !in_quote;
                        j += 1;
                    }
                    '[' if !in_quote => {
                        depth += 1;
                        j += 1;
                    }
                    ']' if !in_quote => {
                        if depth == 0 {
                            // Found matching close bracket
                            let close_pos = byte_offset_to_position(text, j);

                            let open_span = NotationSpan {
                                start: open_pos,
                                end: NotationPosition {
                                    line: open_pos.line,
                                    character: open_pos.character + 1,
                                },
                                start_byte: open_byte,
                                end_byte: open_byte + 1,
                            };
                            let close_span = NotationSpan {
                                start: close_pos,
                                end: NotationPosition {
                                    line: close_pos.line,
                                    character: close_pos.character + 1,
                                },
                                start_byte: j,
                                end_byte: j + 1,
                            };

                            ast.bracket_spans.push((open_span, close_span));
                            j += 1;
                            break;
                        } else {
                            depth -= 1;
                            j += 1;
                        }
                    }
                    _ => {
                        j += 1;
                    }
                }
            }

            i = j;
        } else {
            // Line-based mode: skip non-bracket lines (no bracket spans to emit)
            while i < chars.len() && chars[i] != '\n' && chars[i] != '[' {
                i += 1;
            }
            if i < chars.len() && chars[i] == '\n' {
                i += 1;
            }
        }
    }
}

// =============================================================================
// Helper: Source parsing with spans
// =============================================================================

/// Parse a source pair, extracting names with spans and paren/comma positions.
fn parse_source_with_spans(
    pair: &pest::iterators::Pair<Rule>,
    source: &str,
    offset: usize,
) -> (
    Vec<(String, NotationSpan)>,
    Option<(NotationSpan, NotationSpan)>,
    Vec<NotationSpan>,
) {
    let mut sources = Vec::new();
    let mut paren_spans = None;
    let mut comma_spans = Vec::new();

    // Clone the pair since we need to iterate its inner elements
    let pair = pair.clone();
    let inner = match pair.into_inner().next() {
        Some(p) => p,
        None => return (sources, paren_spans, comma_spans),
    };

    match inner.as_rule() {
        Rule::group => {
            // Group: "(" alias ("," alias)+ ")"
            // The pest span of the group includes parens.
            let group_span = pest_span_to_notation_span(&inner.as_span(), source, offset);

            // Open paren is the first character of the group span
            let open_paren = NotationSpan {
                start: group_span.start,
                end: NotationPosition {
                    line: group_span.start.line,
                    character: group_span.start.character + 1,
                },
                start_byte: group_span.start_byte,
                end_byte: group_span.start_byte + 1,
            };
            // Close paren is the last character
            let close_paren = NotationSpan {
                start: NotationPosition {
                    line: group_span.end.line,
                    character: group_span.end.character.saturating_sub(1),
                },
                end: group_span.end,
                start_byte: group_span.end_byte.saturating_sub(1),
                end_byte: group_span.end_byte,
            };
            paren_spans = Some((open_paren, close_paren));

            // Extract aliases and find commas
            let group_text = inner.as_str();
            let group_start_byte = inner.as_span().start();

            // Find commas in the group text
            let mut in_group = false;
            for (ci, ch) in group_text.char_indices() {
                if ch == '(' {
                    in_group = true;
                    continue;
                }
                if ch == ')' {
                    break;
                }
                if in_group && ch == ',' {
                    let comma_byte = group_start_byte + ci;
                    let comma_pos = byte_offset_to_position(source, comma_byte);
                    comma_spans.push(NotationSpan {
                        start: comma_pos,
                        end: NotationPosition {
                            line: comma_pos.line,
                            character: comma_pos.character + 1,
                        },
                        start_byte: comma_byte + offset,
                        end_byte: comma_byte + offset + 1,
                    });
                }
            }

            for p in inner.into_inner() {
                if p.as_rule() == Rule::alias {
                    let name = p.as_str().to_string();
                    let span = pest_span_to_notation_span(&p.as_span(), source, offset);
                    sources.push((name, span));
                }
            }
        }
        Rule::alias => {
            let name = inner.as_str().to_string();
            let span = pest_span_to_notation_span(&inner.as_span(), source, offset);
            sources.push((name, span));
        }
        _ => {}
    }

    (sources, paren_spans, comma_spans)
}

// =============================================================================
// Helper: Hover formatting
// =============================================================================

/// Render a cap URN's `in` / `out` direction specs followed
/// by every non-direction tag as rows in a property table.
/// Only `in` and `out` have functional meaning on a cap URN;
/// every other tag is arbitrary user-attached data and is
/// enumerated generically here for display without any tag
/// being privileged.
fn push_cap_urn_properties(md: &mut String, cap_urn: &CapUrn) {
    md.push_str("| Property | Value |\n|----------|-------|\n");
    md.push_str(&format!("| in | `{}` |\n", cap_urn.in_spec()));
    md.push_str(&format!("| out | `{}` |\n", cap_urn.out_spec()));
    // `cap_urn.tags` is a `BTreeMap<String, String>` — the
    // iteration order is alphabetical by key, which is
    // stable and reader-friendly. These are the non-
    // direction cap tags (the `CapUrn` parser strips `in` /
    // `out` out of `tags` and into separate fields).
    for (key, value) in cap_urn.tags() {
        md.push_str(&format!("| {} | `{}` |\n", key, value));
    }
}

fn format_alias_hover(
    header: &ParsedHeader,
    cap_details: &HashMap<String, (String, String, Option<String>)>,
) -> String {
    let mut md = format!("**{}** — capability alias\n\n", header.alias);

    md.push_str(&format!("`{}`\n\n", header.cap_urn_str));

    if let Some(ref cap_urn) = header.cap_urn {
        push_cap_urn_properties(&mut md, cap_urn);
    }

    // Registry enrichment
    if let Some((title, description, documentation)) = cap_details.get(&header.cap_urn_str) {
        if !title.is_empty() {
            md.push_str(&format!("\n**{}**\n", title));
        }
        if !description.is_empty() {
            md.push_str(&format!("\n{}\n", description));
        }
        if let Some(doc) = documentation {
            if !doc.is_empty() {
                md.push_str("\n---\n\n");
                md.push_str(doc);
                md.push('\n');
            }
        }
    }

    md
}

fn format_cap_urn_hover(
    cap_urn_str: &str,
    cap_urn: &Option<CapUrn>,
    cap_details: &HashMap<String, (String, String, Option<String>)>,
) -> String {
    let mut md = format!("**Cap URN**\n\n`{}`\n\n", cap_urn_str);

    if let Some(ref cap_urn) = cap_urn {
        push_cap_urn_properties(&mut md, cap_urn);
    }

    if let Some((title, description, documentation)) = cap_details.get(cap_urn_str) {
        if !title.is_empty() {
            md.push_str(&format!("\n**{}**\n", title));
        }
        if !description.is_empty() {
            md.push_str(&format!("\n{}\n", description));
        }
        if let Some(doc) = documentation {
            if !doc.is_empty() {
                md.push_str("\n---\n\n");
                md.push_str(doc);
                md.push('\n');
            }
        }
    }

    md
}

fn format_node_hover(
    node_name: &str,
    node_media: &HashMap<String, MediaUrn>,
    media_details: &HashMap<String, (String, String, Option<String>)>,
) -> String {
    let mut md = format!("**{}** — node\n\n", node_name);

    if let Some(media_urn) = node_media.get(node_name) {
        let urn_str = media_urn.to_string();
        md.push_str(&format!("Media type: `{}`\n", urn_str));

        if let Some((title, description, documentation)) = media_details.get(&urn_str) {
            if !title.is_empty() {
                md.push_str(&format!("\n**{}**\n", title));
            }
            if !description.is_empty() {
                md.push_str(&format!("\n{}\n", description));
            }
            if let Some(doc) = documentation {
                if !doc.is_empty() {
                    md.push_str("\n---\n\n");
                    md.push_str(doc);
                    md.push('\n');
                }
            }
        }
    }

    md
}

// =============================================================================
// Helper: Text scanning utilities
// =============================================================================

/// Find the innermost unclosed `[` before the given byte offset.
fn find_innermost_open_bracket(text: &str, before_offset: usize) -> Option<usize> {
    let mut bracket_stack: Vec<usize> = Vec::new();
    let mut in_quote = false;
    let mut escaped = false;

    for (i, ch) in text.char_indices() {
        if i >= before_offset {
            break;
        }

        if escaped {
            escaped = false;
            continue;
        }

        match ch {
            '\\' if in_quote => {
                escaped = true;
            }
            '"' => {
                in_quote = !in_quote;
            }
            '[' if !in_quote => {
                bracket_stack.push(i);
            }
            ']' if !in_quote => {
                bracket_stack.pop();
            }
            _ => {}
        }
    }

    bracket_stack.last().copied()
}

/// Find the end byte offset of the current statement.
///
/// In bracketed mode this is the matching `]` if present, otherwise EOF. In
/// line-based mode this is the next newline or EOF.
fn find_statement_end(
    text: &str,
    statement_start: usize,
    skip: usize,
    bracket_start: Option<usize>,
) -> usize {
    if let Some(open_bracket) = bracket_start {
        let mut depth = 0usize;
        let mut in_quote = false;
        let mut escaped = false;

        for (i, ch) in text.char_indices() {
            if i < open_bracket {
                continue;
            }

            if escaped {
                escaped = false;
                continue;
            }

            match ch {
                '\\' if in_quote => escaped = true,
                '"' => in_quote = !in_quote,
                '[' if !in_quote => depth += 1,
                ']' if !in_quote => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return i;
                    }
                }
                _ => {}
            }
        }

        return text.len();
    }

    let line_body_start = statement_start + skip;
    text[line_body_start..]
        .find('\n')
        .map(|idx| line_body_start + idx)
        .unwrap_or(text.len())
}

/// Extract the word prefix being typed at the end of a string.
/// Scans backwards from the end to find the start of the current word.
fn extract_word_prefix(s: &str) -> String {
    let trimmed = s.trim_end();
    let mut start = trimmed.len();

    for (i, ch) in trimmed.char_indices().rev() {
        if ch.is_alphanumeric() || ch == '_' || ch == '-' {
            start = i;
        } else {
            break;
        }
    }

    trimmed[start..].to_string()
}

/// Extract the media URN prefix after `in=` or `out=`.
/// Handles both quoted (`in="media:ext=pdf"`) and unquoted (`in=media:ext=pdf`) forms.
fn extract_media_prefix(after_eq: &str) -> String {
    let s = after_eq.trim_start();
    if s.starts_with('"') {
        // Quoted — extract content after the opening quote
        let inner = &s[1..];
        // Return everything up to closing quote or end
        if let Some(close) = inner.find('"') {
            inner[..close].to_string()
        } else {
            inner.to_string()
        }
    } else {
        // Unquoted — take everything until semicolon or end
        if let Some(end) = s.find(';') {
            s[..end].to_string()
        } else {
            s.to_string()
        }
    }
}

/// Convert a NotationSpan to a SemanticTokenInfo.
fn span_to_token(span: &NotationSpan, token_type: SemanticTokenType) -> SemanticTokenInfo {
    let length = if span.start.line == span.end.line {
        span.end.character - span.start.character
    } else {
        // Multi-line token — use byte length as approximation
        span.end_byte - span.start_byte
    };

    SemanticTokenInfo {
        line: span.start.line,
        start_character: span.start.character,
        length,
        token_type,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // TEST8000: Parsing a simple header and wiring produces a valid AST with both statements.
    #[test]
    fn test8000_parse_simple_header_and_wiring() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:enc=utf-8;ext=txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);

        assert!(
            ast.error.is_none(),
            "expected no error, got: {:?}",
            ast.error
        );
        assert_eq!(ast.statements.len(), 2);
        assert_eq!(ast.bracket_spans.len(), 2);

        // Check alias map
        assert!(ast.alias_map.contains_key("extract"));
        let header = &ast.alias_map["extract"];
        assert!(header.cap_urn.is_some());

        // Check node media
        assert!(ast.node_media.contains_key("doc"));
        assert!(ast.node_media.contains_key("text"));
    }

    // TEST8001: Parsing empty notation returns an error in the AST.
    #[test]
    fn test8001_parse_empty_returns_error() {
        let ast = parse_notation_ast("");
        assert!(matches!(ast.error, Some(MachineSyntaxError::Empty)));
        assert!(ast.statements.is_empty());
    }

    // TEST8002: Parsing invalid notation still returns a partial AST alongside the error.
    #[test]
    fn test8002_parse_invalid_returns_partial_ast() {
        let input = "[extract cap:in=broken";
        let ast = parse_notation_ast(input);

        assert!(ast.error.is_some());
        // Should still have bracket spans from manual scanning
        assert!(!ast.bracket_spans.is_empty() || ast.statements.is_empty());
    }

    // TEST8003: The retired `LOOP` keyword is no longer grammar — a wiring that still
    // writes it parses the leading token as the cap alias, so the trailing real cap
    // alias makes the statement structurally invalid. ForEach is never authored.
    #[test]
    fn test8003_loop_keyword_is_not_grammar() {
        let input = concat!(
            r#"[p2t cap:in="media:page";page-to-text;out="media:ext=txt"]"#,
            "\n[pages -> LOOP p2t -> texts]"
        );
        let ast = parse_notation_ast(input);
        // `LOOP` is now just an identifier in cap position; `p2t` after it has no
        // arrow, so the wiring cannot parse as a valid `source -> cap -> target`.
        assert!(
            ast.error.is_some(),
            "`LOOP p2t` must not parse as a valid wiring now that the keyword is retired"
        );
    }

    // TEST8004: Parsing a fan-in group records grouped input sources correctly.
    #[test]
    fn test8004_parse_fan_in_group() {
        let input = concat!(
            r#"[describe cap:in="media:ext=png;image";describe;out="media:ext=txt"]"#,
            "\n[(thumbnail, model_spec) -> describe -> description]"
        );
        let ast = parse_notation_ast(input);

        let wiring = ast
            .statements
            .iter()
            .find_map(|s| {
                if let ParsedStatement::Wiring(w) = s {
                    Some(w)
                } else {
                    None
                }
            })
            .expect("should have a wiring");

        assert_eq!(wiring.sources.len(), 2);
        assert_eq!(wiring.sources[0].0, "thumbnail");
        assert_eq!(wiring.sources[1].0, "model_spec");
        assert!(wiring.paren_spans.is_some());
        assert_eq!(wiring.comma_spans.len(), 1);
    }

    // =========================================================================
    // Completion context detection
    // =========================================================================

    // TEST8005: Completion context after an opening bracket identifies header-start context.
    #[test]
    fn test8005_context_after_open_bracket() {
        let (ctx, _) = get_completion_context("[", 0, 1);
        assert_eq!(ctx, CompletionContextType::HeaderStart);
    }

    // TEST8006: Completion context after the cap prefix identifies cap-URN editing context.
    #[test]
    fn test8006_context_after_cap_prefix() {
        let (ctx, prefix) = get_completion_context("[alias cap:", 0, 11);
        assert_eq!(ctx, CompletionContextType::CapUrn);
        assert!(prefix.starts_with("cap:"));
    }

    // TEST8007: Completion context inside a media URN is recognized correctly.
    #[test]
    fn test8007_context_in_media_urn() {
        let (ctx, prefix) = get_completion_context(r#"[alias cap:in="media:pd"#, 0, 22);
        assert_eq!(ctx, CompletionContextType::MediaUrn);
        assert!(prefix.starts_with("media:"));
    }

    // TEST8008: Completion context after an arrow identifies the expected next token position.
    #[test]
    fn test8008_context_after_arrow() {
        let (ctx, _) = get_completion_context("[doc -> ", 0, 8);
        assert_eq!(ctx, CompletionContextType::WiringTarget);
    }

    // TEST8009: Completion context outside brackets is recognized as the outer notation context.
    #[test]
    fn test8009_context_outside_brackets() {
        let (ctx, _) = get_completion_context("hello", 0, 3);
        assert_eq!(ctx, CompletionContextType::HeaderStart);
    }

    // =========================================================================
    // Semantic tokens
    // =========================================================================

    // TEST8010: Semantic token generation marks the expected token kinds for simple notation.
    #[test]
    fn test8010_semantic_tokens_simple() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:ext=txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);
        let tokens = emit_semantic_tokens(&ast, input);

        // Should have bracket tokens (4 = 2 statements x 2 brackets)
        let bracket_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::Bracket)
            .count();
        assert_eq!(bracket_count, 4);

        // Should have arrow tokens
        let arrow_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::Arrow)
            .count();
        assert!(
            arrow_count >= 2,
            "expected at least 2 arrows, got {}",
            arrow_count
        );

        // Should have alias tokens (extract in header, extract in wiring)
        let alias_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::Alias)
            .count();
        assert!(
            alias_count >= 2,
            "expected at least 2 aliases, got {}",
            alias_count
        );

        // Should have node tokens (doc, text)
        let node_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::Node)
            .count();
        assert!(
            node_count >= 2,
            "expected at least 2 nodes, got {}",
            node_count
        );

        // Should have cap: prefix
        let cap_prefix_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::CapPrefix)
            .count();
        assert_eq!(cap_prefix_count, 1);
    }

    // =========================================================================
    // Hover
    // =========================================================================

    fn entity_at<'a>(
        entities: &'a [NotationEntityInfo],
        line: usize,
        character: usize,
    ) -> &'a NotationEntityInfo {
        entities
            .iter()
            .find(|e| e.range.contains(line, character))
            .expect("expected an entity at the given position")
    }

    // TEST8011: Editor model hover metadata resolves correctly for an alias definition.
    #[test]
    fn test8011_editor_model_entity_hover_for_alias_definition() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:ext=txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);
        let (entities, _) =
            build_editor_model(&ast, &HashMap::new(), &HashMap::new(), &HashMap::new());

        let entity = entity_at(&entities, 0, 1);
        assert_eq!(entity.kind, NotationEntityKind::AliasDefinition);
        let md = entity.hover_markdown.as_deref().unwrap_or("");
        assert!(
            md.contains("extract"),
            "alias hover should mention alias name"
        );
        assert!(
            md.contains("capability alias"),
            "alias hover should identify type"
        );
    }

    // TEST8012: Editor model hover metadata resolves correctly for a wiring source node reference.
    #[test]
    fn test8012_editor_model_entity_hover_for_wiring_source_node() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:ext=txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);
        let (entities, _) =
            build_editor_model(&ast, &HashMap::new(), &HashMap::new(), &HashMap::new());

        let entity = entity_at(&entities, 1, 1);
        assert_eq!(entity.kind, NotationEntityKind::Node);
        let md = entity.hover_markdown.as_deref().unwrap_or("");
        assert!(md.contains("doc"), "node hover should mention node name");
        assert!(md.contains("node"), "node hover should identify type");
    }

    // TEST8013: The editor model derives the ForEach badge from cardinality, not from
    // a keyword. A cap whose primary source node carries a sequence (its producer has
    // a sequence output) but which consumes a scalar is marked `is_loop`; a
    // scalar→scalar cap is not. This is the same `Cap::needs_foreach` rule execution
    // uses, so the editor and the run agree.
    #[test]
    fn test8013_editor_model_derives_foreach_from_cardinality() {
        // `disbind` outputs a sequence of pages; `p2t` consumes one page (scalar).
        // pages -> p2t maps per item → ForEach; doc -> disbind does not.
        let input = concat!(
            r#"[disbind cap:in="media:ext=pdf";disbind;out="media:page"]"#,
            "\n",
            r#"[p2t cap:in="media:page";page-to-text;out="media:ext=txt"]"#,
            "\n[doc -> disbind -> pages]\n[pages -> p2t -> texts]"
        );
        let ast = parse_notation_ast(input);
        assert!(ast.error.is_none(), "got: {:?}", ast.error);

        // Key cap_shapes on the exact canonical URN strings the parser produced, the
        // same keys `build_editor_model` looks up. (input_is_sequence, output_is_sequence)
        let mut cap_shapes: HashMap<String, (bool, bool)> = HashMap::new();
        cap_shapes.insert(
            ast.alias_map.get("disbind").unwrap().cap_urn_str.clone(),
            (false, true),
        );
        cap_shapes.insert(
            ast.alias_map.get("p2t").unwrap().cap_urn_str.clone(),
            (false, false),
        );

        let (_, graph) = build_editor_model(&ast, &HashMap::new(), &HashMap::new(), &cap_shapes);

        let cap_for = |alias: &str| {
            graph
                .iter()
                .find(|e| e.kind == NotationGraphElementKind::Cap && e.label == alias)
                .unwrap_or_else(|| panic!("expected a graph cap element for '{alias}'"))
        };
        assert!(
            !cap_for("disbind").is_loop,
            "scalar source feeding a scalar-input cap must not map"
        );
        assert!(
            cap_for("p2t").is_loop,
            "a sequence source feeding a scalar-input cap must map (ForEach)"
        );
    }

    // TEST8014: The editor model graph includes the expected nodes and edges from parsed notation.
    #[test]
    fn test8014_editor_model_graph_contains_nodes_and_edges() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:ext=txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);
        let (_, graph) =
            build_editor_model(&ast, &HashMap::new(), &HashMap::new(), &HashMap::new());

        let node_count = graph
            .iter()
            .filter(|e| e.kind == NotationGraphElementKind::Node)
            .count();
        let cap_count = graph
            .iter()
            .filter(|e| e.kind == NotationGraphElementKind::Cap)
            .count();
        let edge_count = graph
            .iter()
            .filter(|e| e.kind == NotationGraphElementKind::Edge)
            .count();

        assert_eq!(
            node_count, 2,
            "expected one node element per unique node name"
        );
        assert_eq!(cap_count, 1, "expected one cap element per wiring");
        assert_eq!(edge_count, 2, "expected one input edge and one output edge");
    }

    // TEST8015: Cap alias tokens and arrow tokens share the same graph token identity for the cap.
    #[test]
    fn test8015_editor_model_cap_alias_and_arrows_share_token_id_with_graph_cap() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:ext=txt"]
[doc -> extract -> text]"#;
        let ast = parse_notation_ast(input);
        let (entities, graph) =
            build_editor_model(&ast, &HashMap::new(), &HashMap::new(), &HashMap::new());

        let cap_alias_entity = entities
            .iter()
            .find(|e| e.kind == NotationEntityKind::CapAliasReference)
            .expect("expected a cap alias reference entity");
        let arrow_entities: Vec<&NotationEntityInfo> = entities
            .iter()
            .filter(|e| e.kind == NotationEntityKind::Arrow)
            .collect();
        let graph_cap = graph
            .iter()
            .find(|e| e.kind == NotationGraphElementKind::Cap)
            .expect("expected a graph cap element");

        assert_eq!(
            cap_alias_entity.token_id, graph_cap.token_id,
            "cap alias entity and graph cap must share the same token_id"
        );
        assert_eq!(arrow_entities.len(), 2, "expected two arrow entities");
        for arrow in &arrow_entities {
            assert_eq!(
                arrow.token_id, graph_cap.token_id,
                "arrows in a wiring must share the cap's token_id"
            );
        }

        let graph_edges: Vec<&NotationGraphElementInfo> = graph
            .iter()
            .filter(|e| e.kind == NotationGraphElementKind::Edge)
            .collect();
        for edge in &graph_edges {
            assert_eq!(
                edge.token_id, graph_cap.token_id,
                "graph edges in a wiring must share the cap's token_id"
            );
        }
    }

    // TEST8016: Node references share the same token identity as their graph node in the editor model.
    #[test]
    fn test8016_editor_model_node_references_share_token_id_with_graph_node() {
        // Same node name referenced from two wirings should resolve to
        // ONE logical token and ONE graph node with a shared token_id.
        let input = concat!(
            r#"[a cap:in="media:text";upper;out="media:text"]"#,
            "\n",
            r#"[b cap:in="media:text";lower;out="media:text"]"#,
            "\n",
            "[shared -> a -> mid]\n",
            "[mid -> b -> shared]"
        );
        let ast = parse_notation_ast(input);
        let (entities, graph) =
            build_editor_model(&ast, &HashMap::new(), &HashMap::new(), &HashMap::new());

        let shared_entities: Vec<&NotationEntityInfo> = entities
            .iter()
            .filter(|e| e.kind == NotationEntityKind::Node && e.label == "shared")
            .collect();
        assert_eq!(
            shared_entities.len(),
            2,
            "expected two text-side occurrences of 'shared'"
        );
        let shared_token_id = &shared_entities[0].token_id;
        assert_eq!(
            shared_entities[1].token_id, *shared_token_id,
            "both text occurrences of the same node name must share a token_id"
        );

        let shared_graph_nodes: Vec<&NotationGraphElementInfo> = graph
            .iter()
            .filter(|e| e.kind == NotationGraphElementKind::Node && e.label == "shared")
            .collect();
        assert_eq!(
            shared_graph_nodes.len(),
            1,
            "expected exactly one graph node for repeated node name 'shared'"
        );
        assert_eq!(
            shared_graph_nodes[0].token_id, *shared_token_id,
            "graph node must share its token_id with the text entities for the same node name"
        );
    }

    // =========================================================================
    // Line-based mode
    // =========================================================================

    // TEST8017: Parsing line-based headers and wirings produces the expected AST.
    #[test]
    fn test8017_parse_line_based_header_and_wiring() {
        let input = r#"extract cap:in="media:ext=pdf";extract;out="media:enc=utf-8;ext=txt"
doc -> extract -> text"#;
        let ast = parse_notation_ast(input);

        assert!(
            ast.error.is_none(),
            "expected no error, got: {:?}",
            ast.error
        );
        assert_eq!(ast.statements.len(), 2);
        // Line-based statements have no bracket spans
        assert_eq!(ast.bracket_spans.len(), 0);
        assert!(ast.alias_map.contains_key("extract"));
    }

    // TEST8018: Parsing mixed bracketed and line-based notation works within the same document.
    #[test]
    fn test8018_parse_mixed_bracketed_and_line_based() {
        let input = r#"[extract cap:in="media:ext=pdf";extract;out="media:enc=utf-8;ext=txt"]
doc -> extract -> text"#;
        let ast = parse_notation_ast(input);

        assert!(
            ast.error.is_none(),
            "expected no error, got: {:?}",
            ast.error
        );
        assert_eq!(ast.statements.len(), 2);
        // Only the bracketed statement has bracket spans
        assert_eq!(ast.bracket_spans.len(), 1);
    }

    // TEST8019: Line-based completion recognizes header context at the current cursor.
    #[test]
    fn test8019_line_based_completion_context_header() {
        let (ctx, _) = get_completion_context("extract cap:", 0, 12);
        assert_eq!(ctx, CompletionContextType::CapUrn);
    }

    // TEST8020: Line-based completion recognizes wiring context at the current cursor.
    #[test]
    fn test8020_line_based_completion_context_wiring() {
        let (ctx, _) = get_completion_context("doc -> ", 0, 7);
        assert_eq!(ctx, CompletionContextType::WiringTarget);
    }

    // TEST8021: Line-based completion recognizes an existing wiring source context.
    #[test]
    fn test8021_line_based_completion_context_existing_wiring_source() {
        let (ctx, prefix) = get_completion_context("document -> extract -> text", 0, 3);
        assert_eq!(ctx, CompletionContextType::WiringSource);
        assert_eq!(prefix, "doc");
    }

    // TEST8022: Bracketed completion recognizes an existing wiring source context.
    #[test]
    fn test8022_bracketed_completion_context_existing_wiring_source() {
        let (ctx, prefix) = get_completion_context("[document -> extract -> text]", 0, 4);
        assert_eq!(ctx, CompletionContextType::WiringSource);
        assert_eq!(prefix, "doc");
    }

    // TEST8023: Line-based completion recognizes the start-of-line notation context.
    #[test]
    fn test8023_line_based_completion_context_start() {
        let (ctx, _) = get_completion_context("ex", 0, 2);
        assert_eq!(ctx, CompletionContextType::HeaderStart);
    }

    // TEST8026: Semantic tokens are produced correctly for line-based notation without brackets.
    #[test]
    fn test8026_line_based_semantic_tokens_no_brackets() {
        let input = r#"extract cap:in="media:ext=pdf";extract;out="media:enc=utf-8;ext=txt"
doc -> extract -> text"#;
        let ast = parse_notation_ast(input);
        let tokens = emit_semantic_tokens(&ast, input);

        // No bracket tokens for line-based statements
        let bracket_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::Bracket)
            .count();
        assert_eq!(bracket_count, 0);

        // Should still have other tokens
        let alias_count = tokens
            .iter()
            .filter(|t| t.token_type == SemanticTokenType::Alias)
            .count();
        assert!(
            alias_count >= 2,
            "expected at least 2 aliases, got {}",
            alias_count
        );
    }

    // =========================================================================
    // Position conversion
    // =========================================================================

    // TEST8027: Byte offsets are converted to line and character positions correctly.
    #[test]
    fn test8027_byte_offset_to_position_works() {
        let text = "line0\nline1\nline2";
        assert_eq!(
            byte_offset_to_position(text, 0),
            NotationPosition {
                line: 0,
                character: 0
            }
        );
        assert_eq!(
            byte_offset_to_position(text, 6),
            NotationPosition {
                line: 1,
                character: 0
            }
        );
        assert_eq!(
            byte_offset_to_position(text, 12),
            NotationPosition {
                line: 2,
                character: 0
            }
        );
    }

    // TEST8028: Line and character coordinates are converted back to byte offsets correctly.
    #[test]
    fn test8028_line_char_to_offset_works() {
        let text = "line0\nline1\nline2";
        assert_eq!(line_char_to_offset(text, 0, 0), Some(0));
        assert_eq!(line_char_to_offset(text, 1, 0), Some(6));
        assert_eq!(line_char_to_offset(text, 2, 0), Some(12));
        assert_eq!(line_char_to_offset(text, 2, 5), Some(17));
    }
}
