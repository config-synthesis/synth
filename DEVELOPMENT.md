# Development

## Pipenv Issues

If running `pipenv install`, `pipenv lock`, or `pipenv sync` fails, try doing
the following.

1. Remove `ignored/` (or move it to a temporary directory). A previous version
   of pipenv was picking up on examples in the `ignored/` directory and trying
   to install packages from there.
2. If `numpy` fails to compile on macOS.
   1. Run `pip install --upgrade pip`.
   2. Run `export SYSTEM_VERSION_COMPAT=1`.
   3. Try reinstalling.

To update dependencies after adding package to `setup.py`:

1. Run `pipenv lock`.
2. Run `pipenv sync --dev --keep-outdated`.

## Useful Linux Commands

Find all current overlay mounts.

```
$ findmnt --types=overlay
```

Show all changes to overlay mounts over time. (Note, poll may miss mounts if
they are unmounted immediately, such as happens during the Docker container
creation process.)

```
$ findmnt --poll --types=overlay
```

Display a Docker container's overlayfs directories.

```
$ docker inspect <sha> | jq '.[0].GraphDriver.Data'
```

Unmount a Docker container merged directory.

```
$ sudo umount "$(docker inspect <sha> | jq -r '.[0].GraphDriver.Data.MergedDir')"
```

Intercept Docker API Calls made via `/var/run/docker.sock`.

```
$ sudo mv /var/run/docker.sock /var/run/docker.sock.original
$ sudo socat -t100 -x -v UNIX-LISTEN:/var/run/docker.sock,mode=777,reuseaddr,fork UNIX-CONNECT:/var/run/docker.sock.original
```

Create an overlayfs mount.

```
$ sudo mount -t overlay <name> -o lowerdir=<lowerdir>,upperdir=<upperdir>,workdir=<workdir> <mergeddir>
```

Reset an overlayfs mount.

```
$ sudo umount <mergeddir>
$ rm -rf <upperdir>/* <workdir>/*
$ sudo mount -t overlay <name> -o lowerdir=<lowerdir>,upperdir=<upperdir>,workdir=<workdir> <mergeddir>
```

Unmount all overlayfs mounts.

```
$ umount --all --types=overlay
```

Manually mount `alpine:3.12.1`

```
$ cd /var/lib/docker/overlay2/l
$ mkdir -p /tmp/mount/{diff,merged,work}
$ docker inspect alpine:3.12.1 | jq '.[0].GraphDriver.Data'
{
  "MergedDir": "/var/lib/docker/overlay2/8009c6c39fe2b7b5010fbdf0878aaea3da99d9d4330b66ff42cfeaabbc9065f9/merged",
  "UpperDir": "/var/lib/docker/overlay2/8009c6c39fe2b7b5010fbdf0878aaea3da99d9d4330b66ff42cfeaabbc9065f9/diff",
  "WorkDir": "/var/lib/docker/overlay2/8009c6c39fe2b7b5010fbdf0878aaea3da99d9d4330b66ff42cfeaabbc9065f9/work"
}
$ cat /var/lib/docker/overlay2/8009c6c39fe2b7b5010fbdf0878aaea3da99d9d4330b66ff42cfeaabbc9065f9/link
YTSUVOJTWZG57ILW3AKPI65K7L
$ mount -t overlay overlay_test -o lowerdir=YTSUVOJTWZG57ILW3AKPI65K7L,upperdir=/tmp/mount/diff/,workdir=/tmp/mount/work/ /tmp/mount/merged/ 
```

## Database

To back up the database, run

```
$ docker exec -it synth.mysql sh -c 'mysqldump -Cceq --single-transaction --max-allowed-packet=1G [--password=<root-password>] synth 2>/dev/null' | pv -btra | gzip -9 -c > "ignored/backups/$(date '+%Y-%m-%dT%H:%M:%S').sql.gz"
```

To restore the database, run

```
$ gunzip -c ignored/backups/yyyy-MM-ddTHH:mm:ss.sql.gz | pv -btra | docker exec -i synth.mysql mysql -C --max-allowed-packet=1G [--password=<root-password>] synth
```
