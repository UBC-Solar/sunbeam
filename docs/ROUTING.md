# Routing

Sunbeam is setup to route from `api.sunbeam.ubcsolar.com` to the strategy bay computer at port 8080. Similarly, the endpoint `influxdb.telemetry.ubcsolar.com` is setup to route to the electrical bay computer at some other port. How is this setup?

At a high level, we use Cloudflare for DNS routing and `nginx` as a reverse proxy (directing traffic to the right port). 

Let's say you are trying to route `api.sunbeam.ubcsolar.com` to the strategy bay computer at port 8080.

1. Log into the UBC Solar Cloudflare account and go to DNS Records. 
2. Make a record of Type A where the name is `api.sunbeam.ubcsolar.com` and the target is the strategy bay computer's Tailscale IP (see this with `tailscale status`). Turn Proxied to No/Off, and leave some helpful comment on why this record is being made.
3. Make an `nginx` config file in `/etc/nginx/sites-available/` called `api.sunbeam` as such, 

```
server {
    listen 80;
    server_name api.sunbeam.ubcsolar.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

4. Then, link it to the right spot to be enabled with `sudo ln -s /etc/nginx/sites-available/api.sunbeam /etc/nginx/sites-enabled/`.
5. Check that the configuration worked with `sudo nginx -t`. 
6. Restart `nginx` with `sudo systemctl restart nginx`.

You should be done!