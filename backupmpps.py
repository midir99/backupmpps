#!/usr/bin/env python3

"""
Python script to back up missing person posters data (po_post_url and po_poster_url) in
a S3 bucket.

Usage
-----
    backupmpps.py --help

Example
-------
    backupmpps.py 2022-01-22 2022-05-31 extraviadosbucket
"""

import argparse
import dataclasses
import datetime
import logging
import os
import os.path
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from typing import Any, Optional, final

import boto3
import requests
import urllib3
import urllib3.exceptions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REQUEST_TIMEOUT = 60 * 2
COMPRESS_TIMEOUT = 60 * 2


class ExtraviadosMxApiException(Exception):
    """Use this exception for raising errors related with the Extraviados MX API."""


@dataclasses.dataclass
class Mpp:
    """Represents the missing person poster provided by the Extraviados MX API."""

    id: str
    slug: str
    mp_name: str
    mp_height: Optional[int]
    mp_weight: Optional[int]
    mp_physical_build: str
    mp_complexion: str
    mp_sex: str
    mp_dob: Optional[datetime.date]
    mp_age_when_disappeared: int
    mp_eyes_description: str
    mp_hair_description: str
    mp_outfit_description: str
    mp_identifying_characteristics: str
    circumstances_behind_dissapearance: str
    missing_from: str
    missing_date: Optional[datetime.date]
    found: bool
    alert_type: str
    po_state: str
    po_post_url: str
    po_post_publication_date: Optional[datetime.date]
    po_poster_url: str
    is_multiple: bool
    updated_at: Optional[datetime.datetime]
    created_at: Optional[datetime.datetime]

    @classmethod
    def from_api_dict(cls, api_dict: dict[str, Any]) -> "Mpp":
        """
        Returns an instance of this class from a dictionary provided by parsing the JSON
        response from Extraviados MX API.
        """
        try:
            raw_mp_dob = api_dict["mp_dob"]
            raw_missing_date = api_dict["missing_date"]
            raw_po_post_publication_date = api_dict["po_post_publication_date"]
            raw_updated_at = api_dict["updated_at"]
            raw_created_at = api_dict["created_at"]
        except KeyError as ex:
            raise ExtraviadosMxApiException(
                f"provided dict did not contain the key {ex}"
            ) from ex
        mp_dob = (
            None
            if raw_mp_dob is None
            else datetime.datetime.strptime(raw_mp_dob, "%Y-%m-%d").date()
        )
        missing_date = (
            None
            if raw_missing_date is None
            else datetime.datetime.strptime(raw_missing_date, "%Y-%m-%d").date()
        )
        po_post_publication_date = (
            None
            if raw_po_post_publication_date is None
            else datetime.datetime.strptime(
                raw_po_post_publication_date, "%Y-%m-%d"
            ).date()
        )
        updated_at = (
            None
            if raw_updated_at is None
            else datetime.datetime.fromisoformat(raw_updated_at)
        )
        created_at = (
            None
            if raw_created_at is None
            else datetime.datetime.fromisoformat(raw_created_at)
        )
        mpp = cls(**api_dict)
        mpp.mp_dob = mp_dob
        mpp.missing_date = missing_date
        mpp.po_post_publication_date = po_post_publication_date
        mpp.updated_at = updated_at
        mpp.created_at = created_at
        return mpp


@dataclasses.dataclass
class RetrieveMppsApiBody:
    """
    Represents the response given by the endpoint https://extraviados.mx/api/v1/mpps/
    """

    next: Optional[str]
    previous: Optional[str]
    count: int
    results: list[Mpp]

    @classmethod
    def from_api_dict(cls, api_dict: dict[str, Any]) -> "RetrieveMppsApiBody":
        """
        Returns an instance of this class from a dictionary provided by parsing the JSON
        response given by the endpoint https://extraviados.mx/api/v1/mpps/.
        """
        try:
            results = api_dict["results"]
            next_ = api_dict["next"]
            previous = api_dict["previous"]
            count = api_dict["count"]
        except KeyError as ex:
            raise ExtraviadosMxApiException(
                f"provided dict did not contain the key {ex}"
            ) from ex
        results = [Mpp.from_api_dict(result) for result in results]
        return cls(
            next=next_,
            previous=previous,
            count=count,
            results=results,
        )


def _retrieve_mpps_by_updated_at_date(url: str) -> RetrieveMppsApiBody:
    res = requests.get(url, timeout=REQUEST_TIMEOUT)
    if res.status_code != 200:
        raise ExtraviadosMxApiException(f"{res.url} returned status {res.status_code}")
    try:
        body = res.json()
    except requests.exceptions.JSONDecodeError as ex:
        raise ExtraviadosMxApiException(
            f"unable to parse JSON returned by {res.url}"
        ) from ex
    return RetrieveMppsApiBody.from_api_dict(body)


def retrieve_mpps_by_updated_at_date(
    updated_at_after: datetime.date,
    updated_at_before: datetime.date,
    extraviadosmx_endpoint_url: Optional[str] = None,
) -> list[Mpp]:
    """Retrieves the missing person posters from the Extraviados MX API.

    It will retrieve those mpps whose update_at field in after updated_at_after and
    before updated_at_before.

    You can change the Extraviados MX API endpoint (https://extraviados.mx) by
    providing the parameter extraviadosmx_endpoint_url.
    """
    if extraviadosmx_endpoint_url is None:
        extraviadosmx_endpoint_url = "https://extraviados.mx"
    url = f"{extraviadosmx_endpoint_url}/api/v1/mpps/?" + urllib.parse.urlencode(
        {
            "updated_at_after": updated_at_after.isoformat(),
            "updated_at_before": updated_at_before.isoformat(),
        }
    )
    api_res = _retrieve_mpps_by_updated_at_date(url)
    records = api_res.results
    while api_res.next is not None:
        api_res = _retrieve_mpps_by_updated_at_date(api_res.next)
        records += api_res.results
    return records


def _save_file(response: requests.Response, content_type: str, filename: str) -> str:
    """The return value is the final filename used to save the file."""
    ext = content_type.split("/")[1]
    final_filename = f"{filename}.{ext}"
    with open(final_filename, "wb") as file:
        shutil.copyfileobj(response.raw, file)
    return final_filename


def _save_text_file(response: requests.Response, filename: str) -> str:
    """The return value is the final filename used to save the file."""
    final_filename = f"{filename}.html"
    with open(final_filename, "wt", encoding="utf-8") as file:
        file.write(response.text)
    return final_filename


# TODO: Enhace this function so it can handle more text content-types
def download_url(url: str, filename: str) -> str:
    """
    Please don't include the extension in the filename, it will be appended
    automatically, we will generate it from the Content-Type response header.

    This function supports the following Content-Type response headers:
    - application/pdf
    - image/jpeg
    - image/png
    - text/html; charset=utf-8

    Returns the final filename used to save the file.
    """
    try:
        res = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.SSLError:
        logging.warning("retrieving %s without SSL cert verification", url)
        res = requests.get(url, stream=True, verify=False, timeout=REQUEST_TIMEOUT)
    res.raise_for_status()
    content_type = res.headers.get("Content-Type", "").lower().strip()
    if content_type in ["application/pdf", "image/jpeg", "image/png"]:
        final_filename = _save_file(res, content_type, filename)
    elif content_type in ["text/html; charset=utf-8"]:
        final_filename = _save_text_file(res, filename)
    else:
        raise ValueError(f"Content-Type '{content_type}' is not supported")
    return final_filename


def compress_pdf(filename: str) -> str:
    """Returns the new filename assigned.

    Be careful, this function will replace the original file.
    """
    tmp_filename = f"{filename}.tmp"
    gs_args = [
        "gs",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/screen",
        "-dNOPAUSE",
        "-dQUIET",
        "-dBATCH",
        f"-sOutputFile={tmp_filename}",
        filename,
    ]
    subprocess.run(gs_args, capture_output=True, check=True, timeout=COMPRESS_TIMEOUT)
    mv_args = [
        "mv",
        "--force",
        tmp_filename,
        filename,
    ]
    subprocess.run(mv_args, capture_output=True, check=True, timeout=COMPRESS_TIMEOUT)
    return filename


def _change_extension(filename: str, new_ext: str) -> str:
    """Changes the extension of a file and returns the new filename."""
    root, _ = os.path.splitext(filename)
    final_filename = root + new_ext
    subprocess.run(["mv", "--force", filename, final_filename])
    return final_filename


def compress_png(filename: str) -> str:
    """Returns the new filename assigned.

    Be careful, this function will replace the original file.
    """
    args = ["pngcrush", "-brute", "-ow", filename]
    completed_process = subprocess.run(
        args, capture_output=True, check=True, timeout=COMPRESS_TIMEOUT, text=True
    )
    if "Not a PNG file" in completed_process.stderr:
        raise ValueError("Not a PNG file")
    _, ext = os.path.splitext(filename)
    if ext == ".png":
        return filename
    return _change_extension(filename, ".png")


def compress_jpeg(filename: str) -> str:
    """Returns the new filename assigned.

    Be careful, this function will replace the original file.
    """
    args = ["jpegoptim", "-v", filename]
    try:
        subprocess.run(
            args, capture_output=True, check=True, timeout=COMPRESS_TIMEOUT, text=True
        )
    except subprocess.CalledProcessError as ex:
        if "Not a JPEG file" in (ex.output or ""):
            raise ValueError("Not a JPEG file") from ex
        raise ex from ex
    _, ext = os.path.splitext(filename)
    if ext == ".jpeg":
        return filename
    return _change_extension(filename, ".jpeg")


def _compress_file(filename: str) -> str:
    """Only supports pdf, jpeg and png."""
    _, ext = os.path.splitext(filename)
    final_filename = filename
    if ext == ".pdf":
        try:
            final_filename = compress_pdf(filename)
        except subprocess.CalledProcessError as ex:
            logging.error("unable to compress the file %s: %s", filename, ex.output)
    elif ext in [".jpeg", ".png"]:
        if ext == ".jpeg":
            try:
                final_filename = compress_jpeg(filename)
            except ValueError as ex:
                if "Not a JPEG file" in str(ex):
                    try:
                        final_filename = compress_png(filename)
                        logging.info(
                            "%s was not a JPEG file, it was a PNG, it was compressed and renamed to %s",
                            filename,
                            final_filename,
                        )
                    except Exception:
                        logging.exception("unable to compress the file %s", filename)
                else:
                    logging.exception("unable to compress the file %s", filename)
            except Exception:
                logging.exception("unable to compress the file %s", filename)
        elif ext == ".png":
            try:
                final_filename = compress_png(filename)
            except ValueError as ex:
                if "Not a PNG file" in str(ex):
                    try:
                        final_filename = compress_jpeg(filename)
                        logging.info(
                            "%s was not a PNG file, it was a JPEG, it was compressed and renamed to %s",
                            filename,
                            final_filename,
                        )
                    except Exception:
                        logging.exception("unable to compress the file %s", filename)
                else:
                    logging.exception("unable to compress the file %s", filename)
            except Exception:
                logging.exception("unable to compress the file %s", filename)
    return final_filename


def _process_url(url: str, filename: str, s3client, bucket_name: str):
    logging.info("downloading %s", url)
    try:
        final_filename = download_url(url, filename)
    except Exception:
        logging.exception("error while downloading %s", url)
        return
    try:
        logging.info("trying to compress %s", final_filename)
        final_filename = _compress_file(final_filename)
    except Exception:
        logging.exception("error while compressing %s", final_filename)
    try:
        logging.info("uploading %s to bucket %s ", final_filename, bucket_name)
        s3client.upload_file(
            Filename=final_filename,
            Bucket=bucket_name,
            Key=os.path.basename(final_filename),
            ExtraArgs={"ACL": "public-read"},
        )
    except Exception:
        logging.exception("error while uploading %s to S3 bucket", final_filename)
    try:
        logging.info("deleting %s", final_filename)
        os.remove(final_filename)
    except Exception:
        logging.exception("error while deleting file %s", final_filename)


def backup_mpps(mpps: list[Mpp], s3client, bucket: str):
    """Backs up the missing person posters provided in an S3 bucket.

    1. Downloads the po_post_url and po_poster_url of each missing person poster.
    2. Compresses the downloaded files to save space.
    3. If bucket is specified, uploads those files to an S3 bucket.
    """
    logging.info("%d were retrieved, starting the backup", len(mpps))
    with tempfile.TemporaryDirectory() as tmpdirname:
        logging.info("created temporary directory %s", tmpdirname)
        total_mpps = len(mpps)
        for i, mpp in enumerate(mpps):
            try:
                logging.info("processing %s", mpp.mp_name.upper())
                post_filename = f"{tmpdirname}/{mpp.id}.po_post_url"
                _process_url(mpp.po_post_url, post_filename, s3client, bucket)
                poster_filename = f"{tmpdirname}/{mpp.id}.po_poster_url"
                _process_url(mpp.po_poster_url, poster_filename, s3client, bucket)
            except Exception:
                logging.exception(
                    "an unhandled error happened when processing %s",
                    mpp.mp_name.upper(),
                )
            logging.info("PROGRESS [ %s / %s ]", i + 1, total_mpps)


@dataclasses.dataclass
class ProgramArgs:
    """Represents the arguments of the CLI."""

    datefrom: datetime.datetime
    dateto: datetime.datetime
    bucket: str
    extraviadosmx_endpoint_url: str
    s3_endpoint_url: str
    logfile: str

    def raise_for_invalid_params(self):
        """
        Validates the arguments provided to the CLI and raises a ValueError if they are
        not valid.
        """
        if self.datefrom > self.dateto:
            raise ValueError("datefrom must be before or equal to dateto")


def parse_args() -> ProgramArgs:
    """Parses the arguments of the CLI."""
    parser = argparse.ArgumentParser(
        prog="backupmpps",
        description=(
            "Use this CLI to back up the po_post_url and po_poster_url of missing "
            "person posters. In order to specify which missing person posters will be "
            "affected, you need to pass date-from and date-to arguments to this "
            "program, so it will back up those missing person posters with their "
            "updated_at field that are inside the span date-from to date-to. Remeber "
            "to set the envionment variables AWS_ACCESS_KEY_ID and "
            "AWS_SECRET_ACCESS_KEY."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "datefrom",
        help="A date in isoformat (2022-05-31).",
        type=str,
    )
    parser.add_argument(
        "dateto",
        help="A date in isoformat, needs to be after datefrom.",
        type=str,
    )
    parser.add_argument(
        "bucket",
        default="",
        help="The name of the S3 bucket were files will be uploaded.",
        type=str,
    )
    parser.add_argument(
        "--extraviadosmx-endpoint-url",
        default="https://extraviados.mx",
        dest="extraviadosmx_endpoint_url",
        help=(
            "Are you using this script with a local installation of Extraviados MX? "
            "Use this to set the endpoint URL (i.e. http://localhost:8000)."
        ),
        type=str,
    )
    parser.add_argument(
        "--s3-endpoint-url",
        default="https://us-southeast-1.linodeobjects.com",
        dest="s3_endpoint_url",
        help="Endpoint URL for the S3 bucket",
        type=str,
    )
    parser.add_argument(
        "--logfile",
        default="",
        dest="logfile",
        help="Provide the filename of the logfile, leave blank for console logging.",
        type=str,
    )
    args = parser.parse_args()
    datefrom = datetime.datetime.fromisoformat(args.datefrom)
    dateto = datetime.datetime.fromisoformat(args.dateto)
    program_args = ProgramArgs(
        datefrom=datefrom,
        dateto=dateto,
        bucket=args.bucket,
        extraviadosmx_endpoint_url=args.extraviadosmx_endpoint_url,
        s3_endpoint_url=args.s3_endpoint_url,
        logfile=args.logfile,
    )
    program_args.raise_for_invalid_params()
    return program_args


def config_logging(logfile: str):
    """Configures the logging of this program."""
    logging.basicConfig(
        filename=logfile or None,
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y/%m/%d %I:%M:%S %p",
    )
    logging.captureWarnings(True)


def main():
    """Main function."""
    program_args = parse_args()
    config_logging(program_args.logfile)
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if aws_access_key_id is None:
        logging.error("no AWS_ACCESS_KEY_ID environment variable found")
        sys.exit(1)
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if aws_secret_access_key is None:
        logging.error("no AWS_SECRET_ACCESS_KEY environment variable found")
        sys.exit(1)
    s3client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=program_args.s3_endpoint_url,
    )
    logging.info(
        "retrieving mpps updated between %s and %s",
        program_args.datefrom.strftime("%d-%b-%y %H:%M:%S"),
        program_args.dateto.strftime("%d-%b-%y %H:%M:%S"),
    )
    mpps = retrieve_mpps_by_updated_at_date(
        program_args.datefrom,
        program_args.dateto,
        program_args.extraviadosmx_endpoint_url,
    )
    backup_mpps(mpps, s3client, program_args.bucket)


if __name__ == "__main__":
    main()
