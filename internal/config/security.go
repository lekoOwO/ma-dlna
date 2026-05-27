package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
)

// ValidateSourceURI performs a preflight validation of the source URL.
// NOTE: This is a preflight check only — the actual fetch is done by Music
// Assistant and/or the selected player, which may follow redirects and resolve
// DNS from a different host. In untrusted-LAN deployments, source fetching needs
// a proxy that validates each redirect hop from the fetcher's network context.
func (c *SecurityConfig) ValidateSourceURI(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid source URL: %w", err)
	}

	scheme := u.Scheme
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("unsupported scheme: %s", scheme)
	}

	host := u.Hostname()
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("cannot resolve source host: %w", err)
	}

	for _, ip := range ips {
		if err := c.validateIP(ip); err != nil {
			return err
		}
	}
	return nil
}

func (c *SecurityConfig) validateIP(ip net.IP) error {
	// Check blocked CIDRs first
	for _, cidr := range c.BlockedSourceCIDRs {
		_, nw, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if nw.Contains(ip) {
			return fmt.Errorf("source IP blocked by policy: %s", ip)
		}
	}

	// If allowed CIDRs are set, check private IPs against them.
	// Public IPs are gated by AllowPublicSources below.
	if len(c.AllowedSourceCIDRs) > 0 && ip.IsPrivate() {
		allowed := false
		for _, cidr := range c.AllowedSourceCIDRs {
			_, nw, err := net.ParseCIDR(cidr)
			if err != nil {
				continue
			}
			if nw.Contains(ip) {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("source IP %s not in allowed CIDRs", ip)
		}
	}

	// Block loopback and link-local (unless explicitly allowed)
	if ip.IsLoopback() && !c.AllowLoopbackSources {
		return fmt.Errorf("source IP blocked: %s (loopback)", ip)
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return fmt.Errorf("source IP blocked: %s (link-local)", ip)
	}
	if ip.IsMulticast() || ip.IsUnspecified() {
		return fmt.Errorf("source IP blocked: %s (multicast/unspecified)", ip)
	}

	// Block public IPs if not allowed
	if !ip.IsPrivate() && !c.AllowPublicSources {
		return fmt.Errorf("public source IP blocked: %s", ip)
	}

	return nil
}

func (c *SecurityConfig) ValidateOrReject(rawURL string) error {
	if err := c.ValidateSourceURI(rawURL); err != nil {
		slog.Warn("Source URI rejected by security policy", "uri", safeURL(rawURL), "error", err)
		return err
	}
	return nil
}

func safeURL(raw string) string {
	for i := 0; i < len(raw); i++ {
		if raw[i] == '?' {
			return raw[:i] + "?..."
		}
	}
	return raw
}
