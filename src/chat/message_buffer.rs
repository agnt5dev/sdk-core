use std::time::Instant;

/// Buffer that accumulates streaming tokens and periodically flushes
/// with markdown healing applied.
///
/// Used for post-then-edit streaming delivery to chat platforms:
/// 1. Agent generates tokens
/// 2. Tokens accumulate in this buffer
/// 3. Every `flush_interval_ms`, buffer flushes with healed markdown
/// 4. Language SDK sends `chat.update` with the flushed content
/// 5. On completion, `finalize()` returns the full clean content
pub struct StreamingMessageBuffer {
    accumulated: String,
    last_flush: Instant,
    last_flushed_content: String,
    flush_interval_ms: u64,
}

impl StreamingMessageBuffer {
    /// Create a new buffer with the given flush interval.
    pub fn new(flush_interval_ms: u64) -> Self {
        Self {
            accumulated: String::new(),
            last_flush: Instant::now(),
            last_flushed_content: String::new(),
            flush_interval_ms,
        }
    }

    /// Create a buffer with the default 500ms flush interval.
    pub fn default_interval() -> Self {
        Self::new(500)
    }

    /// Push a new token into the buffer.
    pub fn push(&mut self, token: &str) {
        self.accumulated.push_str(token);
    }

    /// Check whether enough time has passed to flush.
    pub fn should_flush(&self) -> bool {
        self.last_flush.elapsed().as_millis() as u64 >= self.flush_interval_ms
    }

    /// Returns true if the buffer has new content since the last flush.
    pub fn has_new_content(&self) -> bool {
        self.accumulated != self.last_flushed_content
    }

    /// Flush the buffer, returning markdown-healed content.
    /// Resets the flush timer. Returns None if no new content since last flush.
    pub fn flush(&mut self) -> Option<String> {
        if !self.has_new_content() {
            return None;
        }

        self.last_flush = Instant::now();
        let healed = heal_markdown(&self.accumulated);
        self.last_flushed_content = self.accumulated.clone();
        Some(healed)
    }

    /// Finalize the buffer, returning the complete content (no healing needed
    /// since the LLM output is complete).
    pub fn finalize(&mut self) -> String {
        std::mem::take(&mut self.accumulated)
    }

    /// Get the current accumulated content length.
    pub fn len(&self) -> usize {
        self.accumulated.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.accumulated.is_empty()
    }
}

/// Heal incomplete markdown so it renders correctly mid-stream.
///
/// Fixes:
/// - Unclosed bold (`**text` -> `**text**`)
/// - Unclosed italic (`*text` -> `*text*`)
/// - Unclosed inline code (`` `code `` -> `` `code` ``)
/// - Unclosed code blocks (``` ```code ``` -> close the block)
/// - Unclosed strikethrough (`~~text` -> `~~text~~`)
fn heal_markdown(input: &str) -> String {
    let mut result = input.to_string();

    // Count code block fences (```)
    let fence_count = result.matches("```").count();
    if fence_count % 2 != 0 {
        result.push_str("\n```");
    }

    // Only heal inline markers if we're NOT inside an unclosed code block
    if fence_count % 2 == 0 {
        // Count inline code backticks (outside code blocks)
        let backtick_count = count_unescaped_markers(&result, '`');
        if backtick_count % 2 != 0 {
            result.push('`');
        }

        // Count bold markers (**)
        let bold_count = count_double_markers(&result, '*');
        if bold_count % 2 != 0 {
            result.push_str("**");
        }

        // Count strikethrough markers (~~)
        let strike_count = count_double_markers(&result, '~');
        if strike_count % 2 != 0 {
            result.push_str("~~");
        }

        // Count single italic markers (after removing matched **)
        let italic_remaining = count_single_after_doubles(&result, '*');
        if italic_remaining % 2 != 0 {
            result.push('*');
        }
    }

    result
}

/// Count unescaped single-character markers (for backticks).
fn count_unescaped_markers(text: &str, marker: char) -> usize {
    // Skip over triple backticks first
    let no_fences = text.replace("```", "");
    no_fences.chars().filter(|&c| c == marker).count()
}

/// Count double-character markers (for ** bold, ~~ strikethrough).
fn count_double_markers(text: &str, ch: char) -> usize {
    let marker: String = std::iter::repeat(ch).take(2).collect();
    text.matches(&marker).count()
}

/// Count remaining single markers after removing double markers.
fn count_single_after_doubles(text: &str, ch: char) -> usize {
    let double: String = std::iter::repeat(ch).take(2).collect();
    let without_doubles = text.replace(&double, "");
    without_doubles.chars().filter(|&c| c == ch).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heal_unclosed_bold() {
        assert_eq!(heal_markdown("hello **world"), "hello **world**");
    }

    #[test]
    fn test_heal_closed_bold_unchanged() {
        assert_eq!(heal_markdown("hello **world**"), "hello **world**");
    }

    #[test]
    fn test_heal_unclosed_code() {
        assert_eq!(heal_markdown("run `npm install"), "run `npm install`");
    }

    #[test]
    fn test_heal_unclosed_code_block() {
        assert_eq!(
            heal_markdown("```python\nprint('hello')"),
            "```python\nprint('hello')\n```"
        );
    }

    #[test]
    fn test_heal_closed_code_block_unchanged() {
        let input = "```python\nprint('hello')\n```";
        assert_eq!(heal_markdown(input), input);
    }

    #[test]
    fn test_heal_unclosed_strikethrough() {
        assert_eq!(heal_markdown("~~deleted"), "~~deleted~~");
    }

    #[test]
    fn test_heal_complete_markdown_unchanged() {
        let input = "**bold** and *italic* and `code` and ~~strike~~";
        assert_eq!(heal_markdown(input), input);
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(heal_markdown(""), "");
    }

    #[test]
    fn test_buffer_push_and_flush() {
        let mut buf = StreamingMessageBuffer::new(0); // 0ms = always flushable
        buf.push("hello ");
        buf.push("**world");

        assert!(buf.should_flush());
        let content = buf.flush().unwrap();
        assert_eq!(content, "hello **world**"); // healed

        let final_content = buf.finalize();
        assert_eq!(final_content, "hello **world"); // raw, no healing
    }

    #[test]
    fn test_buffer_no_flush_when_no_new_content() {
        let mut buf = StreamingMessageBuffer::new(0);
        buf.push("hello");
        buf.flush(); // first flush
        assert!(buf.flush().is_none()); // no new content
    }
}
