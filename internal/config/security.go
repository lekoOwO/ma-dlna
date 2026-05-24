package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
)

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
	// Block loopback and link-local
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return fmt.Errorf("source IP blocked: %s (loopback/link-local)", ip)
	}

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

	// If allowed CIDRs are set, check against them
	if len(c.AllowedSourceCIDRs) > 0 {
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

	// Block public IPs if not allowed
	if !c.AllowPublicSources && !ip.IsPrivate() {
		return fmt.Errorf("public source IP blocked: %s", ip)
	}

	return nil
}

func (c *SecurityConfig) ValidateOrLog(rawURL string) {
	if err := c.ValidateSourceURI(rawURL); err != nil {
		slog.Warn("Source URI rejected by security policy", "uri", safeURL(rawURL), "error", err)
	}
}

func safeURL(raw string) string {
	for i := 0; i < len(raw); i++ {
		if raw[i] == '?' {
			return raw[:i] + "?..."
		}
	}
	return raw
}
