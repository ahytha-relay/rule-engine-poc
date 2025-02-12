
export const ActionMap = {
  triggerMessage: async ({clientId, ccid, triggerId}) => {
    // http call to trigger message
  },
  dequeueMessage: async (input) => {},
  updateCustomerObject: async (clientId, ccid, fieldName, fieldValue) => {
    // get the customer object from couchbase
    const customer = {};
    // update the customer object
    customer[fieldName] = fieldValue; // needs more to access nested fields
    // save the customer object to couchbase
  },
}