// Detached child started by scheduleUpdateCheck: node update-check-worker.js <storeDir> <url>
import { refreshUpdateCache } from './update-check.js';

const [storeDir, url] = process.argv.slice(2);
if (storeDir) {
  await refreshUpdateCache({ storeDir, url });
}
