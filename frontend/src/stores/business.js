import {batches,mappings,runs} from '../api/business.js';
import {subscriptions} from '../api/subscriptions.js';
export const store={batches:[],mappings:[],runs:[],subscriptions:[],user:null};
export async function refresh(){[store.batches,store.mappings,store.runs,store.subscriptions]=await Promise.all([batches.list(),mappings.list(),runs.list(),subscriptions.list()]);}
