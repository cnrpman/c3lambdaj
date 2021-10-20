# Common Crawler Cleaner with AWS Lambda Java

Adapted from AWS coding sample

The project source includes function code and supporting resources:
- `src/main` - A Java function.
- `build.gradle` - A Gradle build file.
- `build.sh` - Shell scripts to build project.

Use the following instructions to deploy the sample application.

# Requirements
- [Java 8 runtime environment (SE JRE)](https://www.oracle.com/java/technologies/javase-downloads.html)
- [Gradle 5](https://gradle.org/releases/)
- The Bash shell. For Linux and macOS, this is included by default. In Windows 10, you can install the [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10) to get a Windows-integrated version of Ubuntu and Bash.


# Deploy
Build this project by executing `build.sh`  
Then you will find the `java/build/distributions/java.zip` for AWS Lambda Java 8 deployment.  
Must deploy on region 'us-east-1' to fetch the Common Crawl files in local