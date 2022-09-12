# backupmpps

Python script to back up missing person posters data (po_post_url and po_poster_url) in
a S3 bucket.

## Requirements

This script is intended to be used in **Linux systems**, small modifications can make it run on Windows.

If you want the script to try to compress the files before saving them, you need to
install the following programs:

- `ghostscript`, for PDF compression https://www.ghostscript.com/
- `pngcrush`, for PNG compression
- `jpegoptim`, for JPEG compression

You need Python >= 3.9 to run this script (obviously), and you also need to install the script requirements (remember using a virtual environment):

At this time, the script needs you to provide `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (environment variables) and the name of your S3 bucket to store the files, later I'll work on a "S3 bucket free" implementation.

```
$ python3 -m pip install requirements.txt
```

## Usage

Check the usage of the script with the following command:
```
$ python3 backupmpps.py --help
```
### Example

This will back up the missing person posters that were updated after `2022-01-22` and before `2022-05-31` in the bucket `extraviadosmxbucket`.
```
$ export AWS_ACCESS_KEY_ID=<your-aws-access-key-id>
$ export AWS_SECRET_ACCESS_KEY=<your-aws-secret-access-key>

$ python3 backupmpps.py 2022-01-22 2022-05-31 extraviadosmxbucket
```
