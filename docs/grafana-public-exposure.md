# Public read-only Grafana — Caddy setup

Puts the TART Grafana on a public hostname where anyone can view the dashboards, while admin
access stays reachable only from the private network.

Grafana runs on `192.168.20.84:3001`. Caddy already proxies other hosts on `192.168.20.0/24`,
so the route exists.

## Add the site block

In `roles/caddy/defaults/main.yaml`, add to `caddy_sites`:

```yaml
  - name: tart.jamtoaster.network
    config: |
      encode

      # Grafana's admin surface — not reachable from the internet.
      # 404 rather than 403 so the paths don't advertise themselves.
      @admin path /login* /api/admin/* /api/user/auth-tokens/rotate /user/auth-tokens/rotate
      handle @admin {
        respond 404
      }

      handle {
        reverse_proxy 192.168.20.84:3001 {
          # Grafana's basic auth works against /api/* independently of the login
          # form, so blocking /login alone is not enough. This also neutralises
          # API keys and service-account tokens on the public path.
          header_up -Authorization

          transport http {
            read_timeout 60s
          }
        }
      }
```

Nothing else is needed. Caddy provisions TLS, passes `Host` through unmodified, sets the
`X-Forwarded-*` headers, and handles WebSocket upgrades (Grafana Live) on its own.

**Why the deny rules:** `POST /login` is the only way to create a Grafana session, so blocking it
means no login is possible from the internet. Stripping `Authorization` closes the same door for
basic auth and API tokens, which work against `/api/*` independently of the login form. Admins
log in over the VPN at `http://192.168.20.84:3001`, which bypasses these rules.

Grafana is separately configured to give anonymous visitors the read-only **Viewer** role, so
these rules are a second layer rather than the only one.

## Verify after deploying

```sh
HOST=tart.jamtoaster.network

# Dashboards load anonymously, read-only
curl -sS "https://$HOST/" | grep -o '"orgRole":"[^"]*"'                      # -> "Viewer"

# Admin surface is gone
curl -sS -o /dev/null -w '%{http_code}\n' "https://$HOST/login"              # -> 404
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "https://$HOST/login"      # -> 404
curl -sS -o /dev/null -w '%{http_code}\n' "https://$HOST/api/admin/settings" # -> 404

# Basic auth must fail even with the correct password
curl -sS -u admin:<password> "https://$HOST/api/admin/settings"              # -> 401, never JSON
```

And admin access over the VPN must still work:

```sh
curl -sS -o /dev/null -w '%{http_code}\n' http://192.168.20.84:3001/login    # -> 200
```

## One caveat: no rate limiting

Caddy v2 has no built-in rate limit directive, and these dashboards are expensive to serve —
each panel is a live SQL query with no caching, and Postgres allows a single query to run for up
to 500 seconds. A handful of visitors refreshing a 17-panel dashboard is real database load.

`read_timeout 60s` caps how long Caddy waits, but the query keeps running in Postgres regardless.

If this needs throttling, it means building Caddy with
[`mholt/caddy-ratelimit`](https://github.com/mholt/caddy-ratelimit) via
[xcaddy](https://xcaddy.tech/), then adding to the site block:

```caddy
rate_limit {
  zone tart {
    key    {remote_host}
    events 60
    window 5s
  }
}
```

Sized to absorb one dashboard load (~25 requests) while averaging 12 requests/second. The module
has no separate burst parameter, so the window is widened instead.

The alternative is to add response caching in the TART backend, which is being considered
separately and would also cover `https://jamtoaster.network/api/*`.

## Before this goes live (TART side, not Caddy)

- [ ] Grafana anonymous Viewer access enabled and admin `GF_*` settings applied — done in
      `docker-compose.yml.remote`, needs a container restart.
- [ ] **Admin password changed from `admin`.** Setting `GF_ADMIN_PASSWORD` is not sufficient;
      Grafana only reads it when first creating the user. Run:
      ```sh
      docker compose -f docker-compose.yml.remote exec grafana \
        grafana cli --homepath /usr/share/grafana admin reset-admin-password '<password>'
      ```
- [ ] `GF_PUBLIC_URL=https://tart.jamtoaster.network/` set in `.env` on `192.168.20.84`.
- [ ] Hostname confirmed — the existing `jamtoaster.network` catch-all already proxies
      `192.168.20.84:3000`, which is a different service. Do not reuse it.
