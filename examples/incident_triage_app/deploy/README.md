# Deploy the demo to FastAPI Cloud

One command deploys the demo, with the SDK at the commit you have checked out:

```sh
export FASTAPI_CLOUD_TOKEN=<deploy token>
export FASTAPI_CLOUD_APP_ID=<app UUID>
uv run nox -f examples/incident_triage_app/deploy/noxfile.py
```

The session lives in this folder's [`noxfile.py`](noxfile.py), apart from the
root noxfile: it is not a check, and CI never runs it.

The app ID is the UUID next to the app's name in the dashboard header. The
CLI does not take the app's name, and a deploy token cannot look the UUID up.
Keep both values in the environment and never in a file: the repository is
public.

## How it works

The root `pyproject.toml` describes the SDK, and FastAPI Cloud installs what
the deployed folder's `pyproject.toml` lists. The demo needs more than the SDK:
`httpx`, `anthropic` and a FastAPI with `app.frontend()`. So the session runs
[`bundle.py`](bundle.py), which writes a separate folder in a temporary
directory:

```
app/
  pyproject.toml        the SDK's dependencies plus the demo's; Python 3.13;
                        [tool.fastapi] entrypoint
  .fastapicloudignore
  src/redis_fastapi/    the SDK at this commit
  examples/incident_triage_app/
    build.json          the commit, reported by GET /api/health
```

Then it installs the folder's dependencies and imports the app, as the cloud
build will, so a broken import fails before the upload. Last, it runs
`fastapi deploy <folder> --app-id <ID>`.

The root `pyproject.toml` stays as it is: `fastapi deploy` from the repository
root still deploys `examples.main:app`.

## Set up the app, once

1. **Create the app** in the FastAPI Cloud dashboard, or with a first
   interactive deploy, which signs you in and asks for a team and a name:

   ```sh
   uv run examples/incident_triage_app/deploy/bundle.py /tmp/incident-triage --check
   uvx --from "fastapi[standard]" fastapi deploy /tmp/incident-triage
   ```

   Note the app's UUID, for `FASTAPI_CLOUD_APP_ID`.

2. **Link a Redis Cloud database** (Redis 8.6.2 or later). In the app, open
   **Integrations**, click **Connect Integration**, select **Redis Cloud** and
   choose or create a database. FastAPI Cloud sets `REDIS_URL`. Until then the
   page loads and `GET /api/health` reports Redis as unreachable.

3. **Optional: add the model key.** In **Environment Variables**, add
   `ANTHROPIC_API_KEY` with **Secret** on. Without it the scripted planner
   answers. The daily budget of 500 model runs bounds the bill.

4. **Keep one replica.** In **Scaling**, set the maximum replicas to 1. The
   streams stub keeps runs in memory, so a second replica would not see a run
   that the first one started. Remove this limit when the SDK's streams
   feature replaces the stub.

5. **Create a deploy token.** In the app, open **Tokens** and click **Create
   Token**. It lasts at most 365 days. The token is shown once.

## Keep it running

- **Rotate the token before it expires**, or when it may have leaked. A
  deploy with an expired token fails. Regenerating it under **Tokens**
  invalidates the old value at once.
- **Check which commit is live:** `curl https://<app>.fastapicloud.dev/api/health`
  and read `build.commit`.
- **Roll back** in the dashboard: open the **Actions** menu of an earlier
  deployment and select **Rollback**. Traffic stays pinned to it, and to its
  environment variables, until you unpin it. A deployment that fails to start
  never takes traffic: FastAPI Cloud keeps the last good one.
- **Scale to zero** is on by default. The first request after an idle period
  is slow. The streams stub keeps runs in memory, so a scale-down or a deploy
  forgets them: a reconnect then gets `404`. Sessions, the cache and the
  quotas live in Redis and survive both.
