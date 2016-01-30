# s3-logrotate

Tracking requests to your bucket is done by enabling access logging. The logs are located in a folder within the bucket usually and contain multiple files like these:

```
2016-01-27 20:43       398   s3://www.streambrightdata.com/logs/2016-01-27-20-43-18-B4B772125150FE0F
2016-01-27 20:43       398   s3://www.streambrightdata.com/logs/2016-01-27-20-43-22-40703455B055B8E1
```
Amazon periodically writes the access log records to log files, and then uploads log files to your target bucket (same bucket logs/ folder in this case). 

## Usage

run.sh

## License

See LICENSE file.
# s3-logrotate
