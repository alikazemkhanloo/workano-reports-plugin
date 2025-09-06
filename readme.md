## sample config
in
`/etc/wazo-calld/conf.d/otp-plugin.yml`

```yml
otp-plugin:
  token: uuid
  tenant: uuid
  farsi-reader:
    api-key: TheAPIKey
    api-url: https://api.farsireader.com/ArianaCloudService/ReadTextGET

```

then reload wazo-calld service