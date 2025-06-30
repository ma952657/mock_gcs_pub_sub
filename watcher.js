const SERVER = 'http://localhost:4443';
const BUCKET = 'my-bucket';
const CACHE_FILE = './metadata-cache.json';
const pubsub = new PubSub();

const TOPICS = {
  OBJECT_FINALIZE: 'gcs.object.finalize',
  OBJECT_METADATA_UPDATE: 'gcs.object.metadata',
  OBJECT_DELETE: 'gcs.object.delete',
};

let cache = fs.existsSync(CACHE_FILE) ? JSON.parse(fs.readFileSync(CACHE_FILE)) : {};

async function ensureTopics() {
  for (const topicName of Object.values(TOPICS)) {
    const [exists] = await pubsub.topic(topicName).exists();
    if (!exists) {
      await pubsub.createTopic(topicName);
      console.log(`🧵 Created topic: ${topicName}`);
    }
  }
}

const saveCache = () => {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
}

const listObjects = async() => {
  try {
    const res = await axios.get(`${SERVER}/storage/v1/b/${BUCKET}/o`);
    return res.data.items || [];
  } catch (err) {
    console.error('Failed to list objects:', err.message);
    return [];
  }
}

const diffObjects =(currentList) => {
  const currentMap = {};
  currentList.forEach(obj => (currentMap[obj.name] = obj));

  const prevNames = Object.keys(cache);
  const currNames = Object.keys(currentMap);

  const added = currNames.filter(n => !cache[n]);
  const deleted = prevNames.filter(n => !currentMap[n]);
  const updated = currNames.filter(n => {
    return cache[n] && JSON.stringify(cache[n].metadata) !== JSON.stringify(currentMap[n].metadata);
  });

  return { added, deleted, updated, currentMap };
}

const notify = async(eventType, name, metadata = null) => {
  const topicName = TOPICS[eventType];
  const payload = { eventType, bucket: BUCKET, name, metadata };
  const dataBuffer = Buffer.from(JSON.stringify(payload));

  try {
    await pubsub.topic(topicName).publish(dataBuffer);
    console.log(`Published to topic "${topicName}": ${name}`);
  } catch (err) {
    console.error(`Failed to publish to ${topicName}:`, err.message);
  }
}

const pollObjects = () => {
  const current = await listObjects();
  const { added, deleted, updated, currentMap } = diffObjects(current);

  for (const name of added) await notify('OBJECT_FINALIZE', name, currentMap[name]);
  for (const name of deleted) await notify('OBJECT_DELETE', name);
  for (const name of updated) await notify('OBJECT_METADATA_UPDATE', name, currentMap[name]);

  cache = currentMap;
  saveCache();
}

(async () => {
  await ensureTopics();
  setInterval(pollObjects, 10000);
  console.log(`Watching bucket '${BUCKET}' every 10s for changes...`);
})();