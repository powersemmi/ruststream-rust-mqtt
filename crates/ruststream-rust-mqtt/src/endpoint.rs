//! The address a broker connects to, read as a host and a port.

/// The port an MQTT server listens on when the address names none.
pub(crate) const DEFAULT_PORT: u16 = 1883;

/// Why an address names no endpoint the broker connects to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointError<'a> {
    /// The address names a scheme other than `mqtt`.
    Scheme(&'a str),
    /// The port is not a number from 0 to 65535.
    Port,
    /// The address names no host.
    NoHost,
}

/// Reads `address`, written `mqtt://host:port`, `host:port` or `host`, as a host and a port. An IPv6
/// literal keeps its colons inside brackets.
pub(crate) fn split(address: &str) -> Result<(&str, u16), EndpointError<'_>> {
    let rest = match address.split_once("://") {
        Some(("mqtt", rest)) => rest,
        Some((scheme, _)) => return Err(EndpointError::Scheme(scheme)),
        None => address,
    };
    let rest = rest.trim_end_matches('/');
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(':') && !port.ends_with(']') => {
            let port = port.parse().map_err(|_| EndpointError::Port)?;
            (host, port)
        }
        _ => (rest, DEFAULT_PORT),
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() {
        return Err(EndpointError::NoHost);
    }
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_names_a_host_and_a_port() {
        assert_eq!(split("192.168.1.10:1884"), Ok(("192.168.1.10", 1884)));
        assert_eq!(split("mqtt://broker.local"), Ok(("broker.local", 1883)));
        assert_eq!(split("mqtt://[::1]:1885/"), Ok(("::1", 1885)));
        assert_eq!(split("[::1]"), Ok(("::1", 1883)));
    }

    #[test]
    fn an_address_the_broker_cannot_reach_is_refused_with_the_reason() {
        assert_eq!(
            split("mqtts://broker.local:8883"),
            Err(EndpointError::Scheme("mqtts"))
        );
        assert_eq!(split("broker:port"), Err(EndpointError::Port));
        assert_eq!(split(":1883"), Err(EndpointError::NoHost));
    }
}
