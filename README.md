# HLS Converter Bot

Bot de Telegram que convierte videos a HLS multi-calidad (.m4s) y los sube a Todus S3.

## Uso

1. Envia un enlace directo a un video al bot
2. El bot lo descarga, convierte a 240p/360p/480p con segmentos de 1s en m4s
3. Sube todo a s3.todus.cu/stream/hls/<job_id>/
4. Devuelve el enlace al master.m3u8

## Persistencia

El workflow se auto-dispara cada 5h y tiene un watchdog cada 2h de respaldo.
