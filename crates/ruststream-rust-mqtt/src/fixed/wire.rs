//! The packets of the device variant, encoded into the broker's send buffers and read out of its
//! receive buffers through the `zzq-proto` codec, which copies nothing and allocates nothing.

use ruststream::Headers;
use zzq_proto::encode::{self, AckFields, ConnectFields, Sink, SliceSink};
use zzq_proto::packet::decode_body;
use zzq_proto::property::{
    self, CONTENT_TYPE, CORRELATION_DATA, MAXIMUM_PACKET_SIZE, PAYLOAD_FORMAT_INDICATOR, Property,
    RECEIVE_MAXIMUM, RESPONSE_TOPIC, SESSION_EXPIRY_INTERVAL, SUBSCRIPTION_IDENTIFIER,
};
use zzq_proto::{Packet, PacketType, QoS, SubscriptionOptions, Version, varint};

use crate::fixed::config::MqttConfig;
use crate::fixed::error::MqttError;
use crate::fixed::state::Inbound;
use crate::qos::Qos;
use crate::topic::is_mqtt_text;

/// The most properties a PUBLISH of this crate carries: one per header, the payload format
/// indicator, and room for the headers that have a property of their own.
const PUBLISH_PROPERTIES: usize = ruststream_core::HEADER_ENTRIES + 1;

const fn wire_qos(qos: Qos) -> QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

const fn crate_qos(qos: QoS) -> Qos {
    match qos {
        QoS::AtMostOnce => Qos::AtMostOnce,
        QoS::AtLeastOnce => Qos::AtLeastOnce,
        QoS::ExactlyOnce => Qos::ExactlyOnce,
    }
}

/// Saturates a duration in seconds into the field width a property carries.
fn seconds<Field: TryFrom<u64> + Copy>(duration: core::time::Duration, max: Field) -> Field {
    Field::try_from(duration.as_secs()).unwrap_or(max)
}

/// Encodes the CONNECT of a session into `buf`, announcing that the client takes at most
/// `receive_maximum` unacknowledged deliveries and packets of at most `packet` bytes.
pub(crate) fn connect(
    buf: &mut [u8],
    config: &MqttConfig,
    receive_maximum: usize,
    packet: usize,
) -> Result<usize, MqttError> {
    let properties = [
        Property::U32(
            SESSION_EXPIRY_INTERVAL,
            seconds(config.session_expiry(), u32::MAX),
        ),
        Property::U16(
            RECEIVE_MAXIMUM,
            u16::try_from(receive_maximum).unwrap_or(u16::MAX).max(1),
        ),
        Property::U32(
            MAXIMUM_PACKET_SIZE,
            u32::try_from(packet).unwrap_or(u32::MAX),
        ),
    ];
    let fields = ConnectFields {
        version: Version::V5,
        clean_start: config.starts_clean(),
        keep_alive: seconds(config.keep_alive_interval(), u16::MAX),
        properties: &properties,
        client_id: config.client_identifier(),
        will: None,
        username: config.credentials_pair().map(|(user, _)| user),
        password: config
            .credentials_pair()
            .map(|(_, password)| password.as_bytes()),
    };
    let mut sink = SliceSink::new(buf);
    encode::connect(&fields, &mut sink);
    sink.written()
        .map(<[u8]>::len)
        .ok_or_else(|| MqttError::Connect {
            address: config.address(),
            reason: "the CONNECT packet does not fit a send buffer of the broker's memory",
        })
}

/// Encodes a SUBSCRIBE of `filter` (in the share group `share`) carrying the subscription
/// identifier `identifier`.
pub(crate) fn subscribe(
    buf: &mut [u8],
    packet: u16,
    identifier: u32,
    filter: &str,
    share: Option<&str>,
    qos: Qos,
) -> Option<usize> {
    let properties = [Property::VarInt(SUBSCRIPTION_IDENTIFIER, identifier)];
    let properties_len = property::encoded_len(&properties);
    let filter_len = share.map_or(0, |group| "$share/".len() + group.len() + 1) + filter.len();
    let remaining = 2 + varint::len(properties_len) + properties_len + 2 + filter_len + 1;
    let options = SubscriptionOptions {
        qos: wire_qos(qos),
        ..SubscriptionOptions::default()
    };
    let mut sink = SliceSink::new(buf);
    sink.put_header(PacketType::Subscribe.header_byte(), remaining);
    sink.put_u16(packet);
    sink.put_varint(properties_len);
    property::encode_all(&properties, &mut sink);
    sink.put_u16(u16::try_from(filter_len).ok()?);
    if let Some(group) = share {
        sink.put(b"$share/");
        sink.put(group.as_bytes());
        sink.put(b"/");
    }
    sink.put(filter.as_bytes());
    sink.put_u8(options.to_byte(Version::V5));
    sink.written().map(<[u8]>::len)
}

/// Whether a content type names text, which sets the payload format indicator.
pub(crate) fn is_text_media_type(content_type: &str) -> bool {
    let media = content_type.split(';').next().unwrap_or_default().trim();
    let starts = |prefix: &str| {
        media.len() >= prefix.len() && media[..prefix.len()].eq_ignore_ascii_case(prefix)
    };
    let ends = |suffix: &str| {
        media.len() >= suffix.len()
            && media[media.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
    };
    starts("text/") || media.eq_ignore_ascii_case("application/json") || ends("+json")
}

/// One publish as its publisher asked for it.
pub(crate) struct Outgoing<'a> {
    pub(crate) topic: &'a str,
    pub(crate) payload: &'a [u8],
    pub(crate) headers: &'a Headers,
    pub(crate) qos: Qos,
    pub(crate) retain: bool,
    pub(crate) packet: u16,
}

/// Refuses headers no MQTT property carries: a name and a value must be UTF-8 text with no
/// control character, except the correlation data, which is binary on the wire.
pub(crate) fn check_headers(headers: &Headers) -> Result<(), MqttError> {
    for (name, value) in headers.iter() {
        if name == "correlation-id" {
            continue;
        }
        let text = core::str::from_utf8(value).ok();
        if !(is_mqtt_text(name) && text.is_some_and(is_mqtt_text)) {
            return Err(MqttError::Publish(
                "a header is not text an MQTT property carries: its name and value must be UTF-8 \
                 with no control character and no Unicode non-character",
            ));
        }
    }
    Ok(())
}

/// Encodes a PUBLISH into `buf`, which holds at most `limit` bytes of it.
pub(crate) fn publish(
    buf: &mut [u8],
    limit: usize,
    message: &Outgoing<'_>,
) -> Result<usize, MqttError> {
    let mut properties = heapless::Vec::<Property<'_>, PUBLISH_PROPERTIES>::new();
    let mut add = |property| {
        properties
            .push(property)
            .map_err(|_| MqttError::Publish("a publish carries too many properties"))
    };
    for (name, value) in message.headers.iter() {
        // The headers were checked as text, so only the correlation data is binary.
        let text = core::str::from_utf8(value).unwrap_or_default();
        match name {
            "correlation-id" => add(Property::Binary(CORRELATION_DATA, value))?,
            "content-type" => {
                add(Property::Byte(
                    PAYLOAD_FORMAT_INDICATOR,
                    u8::from(is_text_media_type(text)),
                ))?;
                add(Property::Str(CONTENT_TYPE, text))?;
            }
            "reply-to" => {
                if !crate::topic::is_topic_name(text) {
                    return Err(MqttError::Publish("the reply-to header is not a topic"));
                }
                add(Property::Str(RESPONSE_TOPIC, text))?;
            }
            other => add(Property::User(other, text))?,
        }
    }
    let properties_len = property::encoded_len(&properties);
    let id_len = if message.qos == Qos::AtMostOnce { 0 } else { 2 };
    let remaining = 2
        + message.topic.len()
        + id_len
        + varint::len(properties_len)
        + properties_len
        + message.payload.len();
    let size = encode::packet_len(remaining);
    let capacity = limit.min(buf.len());
    if size > capacity || message.topic.len() > encode::MAX_FIELD_LEN {
        return Err(MqttError::PacketTooLarge { size, capacity });
    }
    let mut byte1 = 0x30 | ((wire_qos(message.qos) as u8) << 1);
    if message.retain {
        byte1 |= 0x01;
    }
    let mut sink = SliceSink::new(&mut buf[..capacity]);
    sink.put_header(byte1, remaining);
    sink.put_str(message.topic);
    if id_len != 0 {
        sink.put_u16(message.packet);
    }
    sink.put_varint(properties_len);
    property::encode_all(&properties, &mut sink);
    sink.put(message.payload);
    sink.written()
        .map(<[u8]>::len)
        .ok_or(MqttError::PacketTooLarge { size, capacity })
}

/// Encodes a PUBACK, PUBREC, PUBREL or PUBCOMP of `packet`.
pub(crate) fn ack(buf: &mut [u8; 4], kind: PacketType, packet: u16) -> usize {
    let mut sink = SliceSink::new(buf);
    encode::ack(&AckFields::new(kind, packet), Version::V5, &mut sink);
    sink.written().map_or(0, <[u8]>::len)
}

/// Encodes a PINGREQ.
pub(crate) fn ping(buf: &mut [u8; 4]) -> usize {
    let mut sink = SliceSink::new(buf);
    encode::pingreq(&mut sink);
    sink.written().map_or(0, <[u8]>::len)
}

/// Encodes a DISCONNECT that ends the session normally.
pub(crate) fn disconnect(buf: &mut [u8; 4]) -> usize {
    let mut sink = SliceSink::new(buf);
    encode::reason_only(PacketType::Disconnect, Version::V5, 0, &[], &mut sink);
    sink.written().map_or(0, <[u8]>::len)
}

/// Why a PUBLISH the server sent is not handed to a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unreadable {
    /// The packet breaks the protocol.
    Malformed,
    /// It names its topic by an alias, which the session never announced.
    Alias,
    /// Its headers do not fit the fixed headers of a message.
    Headers,
}

/// Reads the PUBLISH in the first `len` bytes of `inbound`, whose fixed header takes
/// `header_len` of them, into what a delivery needs: the places of the topic and the payload in
/// the buffer, the headers and the subscription identifiers. Nothing of the packet is copied but
/// its headers.
pub(crate) fn read_publish<const PACKET: usize>(
    inbound: &mut Inbound<PACKET>,
    header_len: usize,
    len: usize,
    session: u32,
) -> Result<(), Unreadable> {
    let Inbound {
        bytes,
        topic,
        payload,
        headers,
        qos,
        packet,
        session: arrived_in,
        identifiers,
        ..
    } = inbound;
    let Ok(Packet::Publish(publish)) = decode_body(bytes[0], &bytes[header_len..len], Version::V5)
    else {
        return Err(Unreadable::Malformed);
    };
    // The packet identifier and the quality of service come first, so a refusal below still
    // names the packet to acknowledge.
    *qos = crate_qos(publish.qos);
    *packet = publish.pkid;
    *arrived_in = session;
    if publish.topic.is_empty() {
        return Err(Unreadable::Alias);
    }
    *headers = Headers::new();
    identifiers.clear();
    for property in &publish.properties {
        let inserted = match property {
            Property::User(name, value) => headers.insert(name, value),
            Property::Str(CONTENT_TYPE, value) => headers.insert("content-type", value),
            Property::Str(RESPONSE_TOPIC, value) => headers.insert("reply-to", value),
            Property::Binary(CORRELATION_DATA, value) => headers.insert("correlation-id", value),
            Property::VarInt(SUBSCRIPTION_IDENTIFIER, value) => {
                // A packet that names more subscriptions than a delivery holds reaches them by
                // its topic instead.
                if identifiers.push(value).is_err() {
                    identifiers.clear();
                }
                Ok(())
            }
            _ => Ok(()),
        };
        if inserted.is_err() {
            return Err(Unreadable::Headers);
        }
    }
    // The topic follows the fixed header and its two length bytes; the payload ends the packet.
    let topic_start = header_len + 2;
    *topic = (topic_start, topic_start + publish.topic.len());
    *payload = (len - publish.payload.len(), len);
    Ok(())
}

#[cfg(test)]
mod tests {
    use zzq_proto::decode;

    use super::*;

    #[test]
    fn a_textual_content_type_sets_the_payload_format_indicator() {
        assert!(is_text_media_type("application/json"));
        assert!(is_text_media_type("text/plain; charset=utf-8"));
        assert!(is_text_media_type("application/vnd.api+json"));
        assert!(!is_text_media_type("application/octet-stream"));
    }

    #[test]
    fn a_publish_reads_back_as_the_delivery_it_was() {
        let mut headers = Headers::new();
        headers
            .insert("content-type", "application/json")
            .expect("fits");
        headers
            .insert("correlation-id", [0xff, 0x00])
            .expect("fits");
        headers.insert("trace", "abc").expect("fits");
        let mut inbound = Inbound::<128>::new();
        let len = publish(
            &mut inbound.bytes,
            128,
            &Outgoing {
                topic: "devices/d1/state",
                payload: b"{\"on\":true}",
                headers: &headers,
                qos: Qos::AtLeastOnce,
                retain: false,
                packet: 9,
            },
        )
        .expect("the publish fits");
        read_publish(&mut inbound, 2, len, 3).expect("readable");
        assert_eq!(inbound.topic(), "devices/d1/state");
        assert_eq!(inbound.payload(), b"{\"on\":true}");
        assert_eq!(inbound.packet, 9);
        assert_eq!(inbound.qos, Qos::AtLeastOnce);
        assert_eq!(inbound.headers.get("trace"), Some(&b"abc"[..]));
        assert_eq!(
            inbound.headers.get("correlation-id"),
            Some(&[0xff, 0x00][..])
        );
        assert_eq!(inbound.headers.content_type(), Some("application/json"));
    }

    #[test]
    fn a_publish_past_the_room_of_a_packet_is_refused() {
        let headers = Headers::new();
        let mut buf = [0; 32];
        let refused = publish(
            &mut buf,
            32,
            &Outgoing {
                topic: "t",
                payload: &[0; 40],
                headers: &headers,
                qos: Qos::AtMostOnce,
                retain: false,
                packet: 0,
            },
        );
        assert!(matches!(
            refused,
            Err(MqttError::PacketTooLarge { capacity: 32, .. })
        ));
    }

    #[test]
    fn a_subscribe_names_the_share_group_on_the_wire() {
        let mut buf = [0; 64];
        let len = subscribe(&mut buf, 4, 2, "a/+", Some("g"), Qos::ExactlyOnce).expect("fits");
        let (packet, _) = decode(&buf[..len], Version::V5, 64)
            .expect("decodes")
            .expect("whole");
        let Packet::Subscribe(subscribe) = packet else {
            panic!("not a SUBSCRIBE");
        };
        let (filter, options) = subscribe.filters().next().expect("one filter");
        assert_eq!(filter, "$share/g/a/+");
        assert_eq!(options.qos, QoS::ExactlyOnce);
        assert_eq!(
            subscribe.properties.get_int(SUBSCRIPTION_IDENTIFIER),
            Some(2)
        );
    }
}
