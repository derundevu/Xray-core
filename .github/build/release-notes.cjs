const { execFileSync, spawnSync } = require('node:child_process');

function versionParts(tag) {
  if (!/^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.[1-9][0-9]*$/.test(tag)) return null;
  return tag.slice(1).split('.').map(BigInt);
}

function newer(left, right) {
  for (let i = 0; i < 4; i++) {
    if (left[i] !== right[i]) return left[i] > right[i];
  }
  return false;
}

function selectPreviousRelease(releases, tag, isAncestor) {
  const current = versionParts(tag);
  if (!current) throw new Error('Invalid four-part release tag');
  return releases
    .filter(release => !release.draft && !release.prerelease)
    .map(release => ({ release, parts: versionParts(release.tag_name) }))
    .filter(candidate => candidate.parts && newer(current, candidate.parts))
    .sort((left, right) => newer(left.parts, right.parts) ? -1 : newer(right.parts, left.parts) ? 1 : 0)
    .map(candidate => candidate.release)
    .find(release => isAncestor(release.tag_name));
}

function git(args) {
  return execFileSync('git', args, { encoding: 'utf8' }).trimEnd();
}

function isAncestor(ancestor, commit) {
  const result = spawnSync('git', ['merge-base', '--is-ancestor', ancestor, commit], { encoding: 'utf8' });
  if (result.error) throw result.error;
  if (result.status === 0) return true;
  if (result.status === 1) return false;
  throw new Error(`Cannot inspect Git ancestry: ${result.stderr.trim()}`);
}

function escapeMarkdown(value) {
  return value.replace(/[\\`*_{}\[\]()<>#!|~]/g, '\\$&');
}

function generateNotes({ repo, tag, commit, upstreamTag, upstreamCommit, releases, image, digest }) {
  if (!versionParts(tag) || upstreamTag !== tag.slice(0, tag.lastIndexOf('.'))) throw new Error('Release tag does not match upstream base');
  if (!isAncestor(upstreamCommit, commit)) throw new Error('Upstream base is not an ancestor of the release');
  const previous = selectPreviousRelease(releases, tag, candidate => isAncestor(`refs/tags/${candidate}^{commit}`, commit));
  const excluded = [upstreamCommit];
  if (previous) excluded.push(`refs/tags/${previous.tag_name}^{commit}`);
  // Keep fork merge commits: conflict resolutions may contain changes of their own.
  const log = git(['log', '--reverse', '--topo-order', '--format=%H%x00%s', commit, '--not', ...excluded]);
  const repositoryUrl = `https://github.com/${repo}`;
  const upstreamUrl = 'https://github.com/XTLS/Xray-core';
  const baseTag = previous ? previous.tag_name : upstreamTag;
  const baseUrl = previous ? `${repositoryUrl}/releases/tag/${baseTag}` : `${upstreamUrl}/releases/tag/${baseTag}`;
  const diffBase = previous ? previous.tag_name : upstreamCommit;
  const lines = [
    '## Upstream',
    '',
    `Based on [Xray ${upstreamTag}](${upstreamUrl}/releases/tag/${upstreamTag}).`,
  ];
  if (previous) {
    const previousUpstream = previous.tag_name.slice(0, previous.tag_name.lastIndexOf('.'));
    if (previousUpstream !== upstreamTag) lines.push('', `[Upstream changes: ${previousUpstream}...${upstreamTag}](${upstreamUrl}/compare/${previousUpstream}...${upstreamTag})`);
  }
  lines.push('', '## Fork changes', '', `Since [${baseTag}](${baseUrl}); upstream commits excluded.`, '');
  if (log) {
    for (const entry of log.split('\n')) {
      const separator = entry.indexOf('\0');
      const sha = entry.slice(0, separator);
      const subject = entry.slice(separator + 1);
      lines.push(`- ${escapeMarkdown(subject)} ([${sha.slice(0, 7)}](${repositoryUrl}/commit/${sha}))`);
    }
  } else {
    lines.push('No new fork commits.');
  }
  lines.push(
    '',
    `**Full diff:** [${baseTag}...${tag}](${repositoryUrl}/compare/${diffBase}...${tag})`,
    '',
    '## Build',
    '',
    `- Version: \`${tag.slice(1)}\``,
    `- Commit: [\`${commit}\`](${repositoryUrl}/commit/${commit})`,
    `- Docker: \`${image}:${tag.slice(1)}\``,
  );
  if (digest) lines.push(`- Image digest: \`${image}@${digest}\``);
  return lines.join('\n');
}

// Read-only local preview. Resolve the original upstream tag without creating local tags.
function preview(tag, repo) {
  if (!versionParts(tag) || !repo) throw new Error('Usage: node .github/build/release-notes.cjs <vX.Y.Z.N> <owner/repo>');
  const upstreamTag = tag.slice(0, tag.lastIndexOf('.'));
  const refs = git(['ls-remote', '--tags', 'https://github.com/XTLS/Xray-core.git', `refs/tags/${upstreamTag}`, `refs/tags/${upstreamTag}^{}`])
    .split('\n').filter(Boolean).map(line => line.split('\t'));
  const upstream = refs.find(([, ref]) => ref.endsWith('^{}')) || refs.find(([, ref]) => ref === `refs/tags/${upstreamTag}`);
  if (!upstream) throw new Error(`Upstream tag ${upstreamTag} does not exist`);
  const releases = JSON.parse(execFileSync('gh', ['api', `repos/${repo}/releases?per_page=100`, '--paginate', '--slurp'], { encoding: 'utf8' })).flat();
  const notes = generateNotes({ repo, tag, commit: git(['rev-parse', 'HEAD']), upstreamTag, upstreamCommit: upstream[0], releases, image: `ghcr.io/${repo.toLowerCase()}` });
  process.stderr.write('Preview only; the image digest is added after the release build.\n');
  process.stdout.write(`${notes}\n`);
}

module.exports = { versionParts, newer, selectPreviousRelease, generateNotes };

if (require.main === module) {
  try {
    if (process.argv.length !== 4) throw new Error('Usage: node .github/build/release-notes.cjs <vX.Y.Z.N> <owner/repo>');
    preview(process.argv[2], process.argv[3]);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
