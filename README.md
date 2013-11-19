hadoop-watermarking
===================

MapReduce Job for applying an image watermark on a video

To build:
mvn package

To run:
Your hadoop cluster should be configured not to launch reduce tasks before all maps are done.
To do so set the following configuration:

mapred.reduce.slowstart.completed.maps : 1.0

Execute:
hadoop jar hadoop-watermarking-0.1-job.jar input-video watermark-image output-video

Checked to work with .mp4 videos and .png images.
