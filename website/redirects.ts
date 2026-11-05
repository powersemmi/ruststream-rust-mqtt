// Pages of an earlier site, by their address within a version, mapped to their target relative to
// the build's base URL. Every language of a build gets the same set. `docsRs` is the crate's
// rustdoc for the version built, with a trailing slash.
export function redirects(docsRs: string): Record<string, string> {
  void docsRs;
  return {};
}
