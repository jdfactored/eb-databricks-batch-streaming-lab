# Asset bundle — B04 Step 4

`databricks.yml` is complete. `resources/batch_pipeline.job.yml` has one worked task and
two you need to fill in.

```bash
databricks configure              # once, if you have not already
databricks bundle validate        # catches YAML and reference errors before deploying
databricks bundle deploy --target dev
databricks bundle run batch_pipeline_job
```

`databricks bundle validate` is the fast feedback loop — run it after every edit rather
than discovering a typo during deployment.

Before deploying, upload the `notebooks/` folder to the workspace path in the
`notebook_root` variable, or change the variable to wherever you actually put them.

No CLI access? Build the same three-task job in the Jobs UI and submit a screenshot of the
task graph instead. The point of the exercise is the dependency structure and the
parameterisation, not the tool.
