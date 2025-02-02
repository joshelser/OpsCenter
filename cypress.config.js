const { defineConfig } = require("cypress");

module.exports = defineConfig({
  env: {
    OPSCENTER_URL: "http://localhost:8501",
    SNOWFLAKE_ACCOUNT: "",
  },
  e2e: {
    supportFile: false,
    defaultCommandTimeout: 20000,
    watchForFileChanges: false,
    experimentalStudio: true,
    experimentalRunAllSpecs: true,
  },
  retries: {
    // Configure retry attempts for `cypress run`
    runMode: 2,
    // Configure retry attempts for `cypress open`
    openMode: 0,
  },
});
