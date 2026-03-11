def ensure_spark_jars():
    import urllib.request
    from pathlib import Path

    JARS_DIR = Path(__file__).parent.parent / "spark_jars"
    JARS_DIR.mkdir(exist_ok=True)

    MAVEN = "https://repo1.maven.org/maven2"
    jars = {
        "hadoop-aws-3.4.1.jar": f"{MAVEN}/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar",
        "wildfly-openssl-1.1.3.Final.jar": f"{MAVEN}/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar",
        "bundle-2.24.6.jar": f"{MAVEN}/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar"
    }

    for filename, url in jars.items():
        dest = JARS_DIR / filename
        if not dest.exists():
            print(f"Downloading {filename}...")
            urllib.request.urlretrieve(url, dest)


if __name__ == '__main__':
    from pipeline.app import Pipeline

    ensure_spark_jars()
    Pipeline().run()
