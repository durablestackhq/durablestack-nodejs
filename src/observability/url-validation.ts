/**
 * Recognizes loopback hosts the same way .NET's `Uri.IsLoopback` does: the
 * `localhost` name, the entire 127.0.0.0/8 IPv4 range, and the IPv6 loopback
 * address (with or without the bracket notation `URL.hostname` uses for it).
 */
function isLoopbackHost(hostname: string): boolean {
  const normalized = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (normalized === "localhost" || normalized === "::1") {
    return true;
  }
  return /^127(\.\d{1,3}){3}$/.test(normalized);
}

/**
 * Ingestion and runtime-control requests carry tenant credentials in headers,
 * so cleartext HTTP is refused except for loopback addresses, which are
 * useful when developing against a local ingestion endpoint. Mirrors the
 * .NET runtime's `IngestionEventSyncHostedService.ValidateIngestionBaseUrl`.
 */
export function assertSecureEndpoint(url: URL, optionName: string): void {
  if (url.protocol === "https:") {
    return;
  }

  if (url.protocol === "http:" && isLoopbackHost(url.hostname)) {
    return;
  }

  throw new Error(
    `${optionName} must use https (the request carries tenant credentials); http is only permitted for loopback addresses (localhost, 127.0.0.0/8, ::1). Got '${url.origin}'.`
  );
}
