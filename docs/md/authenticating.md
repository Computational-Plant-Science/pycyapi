# Authenticating

The `pycyapi` CLI must obtain an access token to authenticate with CyVerse. The token may be provided to commands via the `--token` parameter, or set as an environment variable `CYVERSE_TOKEN`. An access token can be obtained from the Terrain API by sending a request with basic auth headers (valid CyVerse username and password):

```shell
GET https://de.cyverse.org/terrain/token/cas
```

A `token` command is provided as convenient alternative to manually obtaining a token:

```shell
pycyapi token --username <username> --password <password>
```

The token is printed and can be used with the `--token` option to authenticate subsequent commands.

**Note:** the `token` command is the only one to return plain text &mdash; all other commands return JSON.