import app, { bootstrapPromise } from '../server.js';

export default async function handler(req, res) {
  await bootstrapPromise;
  return app(req, res);
}
