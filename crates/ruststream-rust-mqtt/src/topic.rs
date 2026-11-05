//! The protocol's rules for topic names and topic filters: which strings a publish may name,
//! which a subscription may name, and which topics a filter matches.

/// Whether `text` is text an MQTT string carries without a server closing the connection on it:
/// no control character and no Unicode non-character.
pub(crate) fn is_mqtt_text(text: &str) -> bool {
    // Printable ASCII, which almost every topic and header is, answers in one pass over the bytes.
    if text.bytes().all(|byte| (0x20..0x7f).contains(&byte)) {
        return true;
    }
    text.chars().all(|ch| {
        let code = u32::from(ch);
        let control = code < 0x20 || (0x7f..=0x9f).contains(&code);
        let non_character = (0xfdd0..=0xfdef).contains(&code) || code & 0xfffe == 0xfffe;
        !control && !non_character
    })
}

/// Whether `topic` is a name a publish may carry: not empty, and no wildcard in it.
pub(crate) fn is_topic_name(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains(['+', '#', '\0'])
}

/// Whether `filter` is a filter a subscription may carry: `+` takes a whole level, and `#` takes
/// the whole last one.
pub(crate) fn is_topic_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.contains('\0') {
        return false;
    }
    let mut levels = filter.split('/').peekable();
    while let Some(level) = levels.next() {
        let last = levels.peek().is_none();
        match level {
            "+" => {}
            "#" if last => {}
            other if other.contains(['+', '#']) => return false,
            _ => {}
        }
    }
    true
}

/// Whether a server delivers a message published to `topic` to a subscription on `filter`.
///
/// A topic that starts with `$` is matched only by a filter that starts with `$` too, never by
/// one that starts with a wildcard.
pub(crate) fn matches(topic: &str, filter: &str) -> bool {
    if topic.starts_with('$') != filter.starts_with('$') {
        return false;
    }
    let mut topic_levels = topic.split('/');
    for level in filter.split('/') {
        match level {
            "#" => return true,
            "+" => {
                if topic_levels.next().is_none() {
                    return false;
                }
            }
            literal => {
                if topic_levels.next() != Some(literal) {
                    return false;
                }
            }
        }
    }
    topic_levels.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wildcard_is_subscribe_only() {
        assert!(is_topic_name("devices/dev42/state"));
        assert!(!is_topic_name("devices/+/state"));
        assert!(!is_topic_name("devices/#"));
        assert!(!is_topic_name(""));
    }

    #[test]
    fn a_wildcard_takes_a_whole_level() {
        assert!(is_topic_filter("devices/+/state"));
        assert!(is_topic_filter("devices/#"));
        assert!(is_topic_filter("#"));
        assert!(!is_topic_filter("devices/#/state"));
        assert!(!is_topic_filter("devices/dev+/state"));
        assert!(!is_topic_filter("devices/dev#"));
        assert!(!is_topic_filter(""));
    }

    #[test]
    fn a_filter_matches_level_by_level() {
        assert!(matches("devices/dev42/state", "devices/+/state"));
        assert!(matches("devices/dev42/state", "devices/#"));
        assert!(matches("devices", "devices/#"));
        assert!(!matches("devices/dev42/state", "devices/+"));
        assert!(!matches("devices/dev42", "devices/+/state"));
        assert!(matches("a/b", "a/b"));
        assert!(!matches("a/b", "a/c"));
    }

    #[test]
    fn a_dollar_topic_is_reached_only_by_a_dollar_filter() {
        assert!(matches("$SYS/broker/uptime", "$SYS/#"));
        assert!(!matches("$SYS/broker/uptime", "#"));
        assert!(!matches("$SYS/broker/uptime", "+/broker/uptime"));
    }

    #[test]
    fn a_control_character_or_a_non_character_is_not_mqtt_text() {
        assert!(is_mqtt_text("devices/zürich"));
        assert!(!is_mqtt_text("devices/a\tb"));
        assert!(!is_mqtt_text("devices/\u{fffe}"));
        assert!(!is_mqtt_text("devices/\u{fdd0}"));
    }
}
