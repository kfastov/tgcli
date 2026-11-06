import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Agent worktrees live under .claude/ inside the checkout, each with its own tests.
    exclude: [...configDefaults.exclude, '.claude/**'],
  },
});
