import jsonata from 'jsonata';
import { KafkaJS } from '@confluentinc/kafka-javascript';
const Kafka = KafkaJS.Kafka;


// SQS mock logic --------------------------------------------------------------
const eventQueue = [];
function enqueueActionEvent(event) {
  eventQueue.push(event);
}
async function dequeueActionEvent() {
  // SQS may natively support this kind of polling logic
  const event = eventQueue.shift(); // would be an SQS call
  if(event === undefined ) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    return await dequeueActionEvent();
  }
  return event;
}
// End SQS mock logic ----------------------------------------------------------

// test object definitions -----------------------------------------------------
const testConfig = {
  event_type: 'smarttriggers',
  event_subtype: 'configure',
  rules: [
    {
      predicate: '$boolean($.event_type="powerups_data") and $boolean($.event_subtype="received") and $boolean($.form_id="foo")',
      actions: [
        { action: 'triggerMessage', transform: '{"clientId": $.client_id, "ccid": $.ccid, "triggerId": "12345"}' },
        { action: 'updateCustomerObject', transform: '{"clientId": $.client_id, "ccid": $.ccid, "fieldName": "birthday", "fieldValue": $.answers[5].value}' }
      ]
    },
    {
      predicate: '$boolean($.event_type="consent") and $boolean($.event_subtype="updated") and $boolean($.consent_type="stop")',
      actions: [
        { action: 'dequeueMessage', transform: '{"clientId": $.client_id, "ccid": $.ccid, "triggerId": "12345}' }
      ]
    }
  ]
}

// end test object definitions -------------------------------------------------


const configSchemaCheck = jsonata('$boolean(event_type="smarttriggers") and $boolean(event_subtype="configure") and $boolean($type(rules)="array"');
let ruleSet = [];
async function processEvent( event ) {
  if( await configSchemaCheck.evaluate(event) ) {
    ruleSet = event.rules.map( (rule) => ({
      predicate: jsonata(rule.predicate).evaluate,
      actions: rule.actions.map( ({action, transform}) => ({action, transform: jsonata(transform).evaluate}))
    }));
    // we don't actually need to process the action configuration here, but we
    // might want to validate that the actions exist. Ideally, we would also be
    // able to check that the transform output matches the action input.
    return;
  }

  await Promise.all(ruleSet
    .filter( async ({predicate}) => await predicate(event) ))
    .flatmap( ({actions}) => actions )
    .map( async ({action, transform}) => ({action, input: await transform(event)}) )
    .map(enqueueActionEvent);
}

async function startEventConsumer() {
  const kafkaConfig = {};
  const consumer = new Kafka().consumer(kafkaConfig);
  await consumer.connect();
  await consumer.subscribe({topics: [process.env.KAFKA_TOPIC]});
  consumer.run({
    eachMessage: async ({ message }) => { await processEvent(JSON.parse(message)); }
  });
}

async function startActionProcessor() {
  // no async setup is actually required, but it could happen
  new Promise( async () => {
    while(true) {
      const {action, input} = await dequeueActionEvent();
      await ActionMap[action](input);
    }
  });
  return;
}

async function main() {
  await startEventConsumer();
  await startActionProcessor();
}
main();
