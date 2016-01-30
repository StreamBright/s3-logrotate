# s3-logrotate

Tracking requests to your bucket is done by enabling access logging. The logs are located in a folder within the bucket usually and contain multiple files like these:

```
2016-01-27 20:43       398   s3://www.streambrightdata.com/logs/2016-01-27-20-43-18-B4B772125150FE0F
2016-01-27 20:43       398   s3://www.streambrightdata.com/logs/2016-01-27-20-43-22-40703455B055B8E1
```
Amazon periodically writes the access log records to log files, and then uploads log files to your target bucket (same bucket logs/ folder in this case). 

## Usage

```
lein run
2016-01-30 23:42:01,338 INFO s3-logrotate.core 2015-12-01
2016-01-30 23:42:01,459 INFO s3-logrotate.core 2015-12-02
2016-01-30 23:42:01,638 INFO s3-logrotate.core 2015-12-03
2016-01-30 23:42:01,766 INFO s3-logrotate.core 2015-12-04
2016-01-30 23:42:01,974 INFO s3-logrotate.core 2015-12-05
2016-01-30 23:42:02,107 INFO s3-logrotate.core 2015-12-06
2016-01-30 23:42:02,223 INFO s3-logrotate.core 2015-12-07
2016-01-30 23:42:02,689 INFO s3-logrotate.core 2015-12-08
2016-01-30 23:42:02,904 INFO s3-logrotate.core logs/2015-12-08-04-47-16-535D0D2106B9B047
2016-01-30 23:42:02,907 INFO s3-logrotate.core (f2b98d9dd4d99c07ad532dc8a7daf9639e5362e084f9d3ac74f67ed516040f03 www.streambrightdata.com [08/Dec/2015:03:40:14 +0000] 10.144.194.145 3272ee65a908a7677109fedda345db8d9554ba26398b2ca10581de88777e2b61 5B49BD4E3BE026CD REST
```

## License

See LICENSE file.
# s3-logrotate
