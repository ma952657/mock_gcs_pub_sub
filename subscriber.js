const { PubSub } = require('@google-cloud/pubsub');
const pubsub = new PubSub();

const subscriptions = [
  { topic: 'gcs.object.finalize', sub: 'sub.finalize' },
  { topic: 'gcs.object.metadata', sub: 'sub.metadata' },
  { topic: 'gcs.object.delete', sub: 'sub.delete' },
];

async function setup() {
  for (const { topic, sub } of subscriptions) {
    const [exists] = await pubsub.subscription(sub).exists();
    if (!exists) {
      await pubsub.topic(topic).createSubscription(sub);
      console.log(`🔔 Subscription created: ${sub}`);
    }

    pubsub.subscription(sub).on('message', message => {
      console.log(`📨 [${sub}] ${message.data.toString()}`);
      message.ack();
    });
  }
}

setup().catch(console.error);
