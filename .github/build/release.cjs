const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

async function getRelease(github, repo, tag) {
  try {
    return (await github.rest.repos.getReleaseByTag({ ...repo, tag })).data;
  } catch (error) {
    if (error.status !== 404) throw error;
    // The tag endpoint can omit drafts; include authenticated draft releases.
    return (await github.paginate(github.rest.repos.listReleases, { ...repo, per_page: 100 }))
      .find(release => release.tag_name === tag);
  }
}

exports.preflight = async ({ github, context }) => {
  const release = await getRelease(github, context.repo, process.env.RELEASE_TAG);
  if (release && !release.draft) throw new Error('Refusing to overwrite a published release');
};

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

exports.publish = async ({ github, context }) => {
  const repo = context.repo;
  const tag = process.env.RELEASE_TAG;
  const version = process.env.SOURCE_VERSION;
  const commit = process.env.RELEASE_COMMIT;
  const image = process.env.RELEASE_IMAGE;
  const digest = process.env.RELEASE_DIGEST;
  const parts = versionParts(tag);
  if (!parts || tag !== `v${version}` || !/^[0-9a-f]{40}$/.test(commit)) throw new Error('Invalid release identity');
  if (image !== `ghcr.io/${repo.owner}/${repo.repo}`.toLowerCase() || !/^sha256:[0-9a-f]{64}$/.test(digest)) throw new Error('Invalid Docker output');
  let object = (await github.rest.git.getRef({ ...repo, ref: `tags/${tag}` })).data.object;
  while (object.type === 'tag') object = (await github.rest.git.getTag({ ...repo, tag_sha: object.sha })).data.object;
  if (object.type !== 'commit' || object.sha !== commit) throw new Error('Release tag moved after validation');
  let release = await getRelease(github, repo, tag);
  if (release && !release.draft) throw new Error('Refusing to overwrite a published release');

  const files = fs.readdirSync('release-assets').sort();
  const names = Object.entries(JSON.parse(fs.readFileSync('.github/build/friendly-filenames.json', 'utf8')))
    .filter(([key]) => !key.endsWith('softfloat')).map(([, value]) => value.friendlyName);
  names.push('android-amd64', 'win7-64', 'win7-32');
  const expected = names.map(name => `Xray-${name}.zip`).sort();
  const actual = files.filter(name => name.endsWith('.zip'));
  if (JSON.stringify(actual) !== JSON.stringify(expected) || !files.includes('SHA256SUMS')) throw new Error('Incomplete release archive set');
  if (files.some(name => !expected.includes(name) && !expected.some(zip => name === `${zip}.dgst`) && name !== 'SHA256SUMS')) throw new Error('Unexpected release asset');

  const generated = (await github.rest.repos.generateReleaseNotes({ ...repo, tag_name: tag, target_commitish: commit })).data.body;
  const body = [
    `Upstream base: [${process.env.UPSTREAM_TAG}](https://github.com/XTLS/Xray-core/releases/tag/${process.env.UPSTREAM_TAG})`,
    `Source version: ${version}`,
    `Commit: ${commit}`,
    `Docker: \`${image}:${version}\` (\`${image}@${digest}\`)`,
    '', generated,
  ].join('\n');
  if (!release) {
    release = (await github.rest.repos.createRelease({ ...repo, tag_name: tag, target_commitish: commit, name: `YAXC ${version}`, body, draft: true, prerelease: false })).data;
  } else {
    release = (await github.rest.repos.updateRelease({ ...repo, release_id: release.id, name: `YAXC ${version}`, body, draft: true, prerelease: false })).data;
  }
  const assertDraft = async () => {
    const current = (await github.rest.repos.getRelease({ ...repo, release_id: release.id })).data;
    if (!current.draft) throw new Error('Release is no longer a draft');
  };
  await assertDraft();
  const oldAssets = await github.paginate(github.rest.repos.listReleaseAssets, { ...repo, release_id: release.id, per_page: 100 });
  for (const asset of oldAssets) {
    if (!files.includes(asset.name)) await github.rest.repos.deleteReleaseAsset({ ...repo, asset_id: asset.id });
  }
  execFileSync('gh', ['release', 'upload', tag, ...files.map(file => path.join('release-assets', file)), '--repo', `${repo.owner}/${repo.repo}`, '--clobber'], { stdio: 'inherit' });

  const inspect = reference => execFileSync('docker', ['buildx', 'imagetools', 'inspect', reference, '--format', '{{.Manifest.Digest}}'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  const versionImage = `${image}:${version}`;
  let existingDigest;
  try {
    existingDigest = inspect(versionImage);
  } catch (error) {
    const message = String(error.stderr);
    const missingReference = message.trim() === `ERROR: ${versionImage}: not found`;
    if (!missingReference && !/\b(?:MANIFEST_UNKNOWN|NAME_UNKNOWN|manifest unknown)\b/i.test(message)) throw error;
  }
  if (existingDigest && existingDigest !== digest) throw new Error('Immutable Docker version tag already points to a different digest');
  if (!existingDigest) execFileSync('docker', ['buildx', 'imagetools', 'create', '--tag', versionImage, `${image}@${digest}`], { stdio: 'inherit' });
  if (inspect(versionImage) !== digest) throw new Error('Docker version digest does not match the build');

  const releases = await github.paginate(github.rest.repos.listReleases, { ...repo, per_page: 100 });
  const latest = releases.filter(item => !item.draft && !item.prerelease)
    .map(item => versionParts(item.tag_name)).filter(Boolean)
    .every(other => newer(parts, other));
  await assertDraft();
  if (latest) execFileSync('docker', ['buildx', 'imagetools', 'create', '--tag', `${image}:latest`, `${image}@${digest}`], { stdio: 'inherit' });
  await github.rest.repos.updateRelease({ ...repo, release_id: release.id, draft: false, prerelease: false, make_latest: latest ? 'true' : 'false' });
};
