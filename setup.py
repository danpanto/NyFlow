def setenv():
    import os
    from pathlib import Path
    from urllib.request import urlretrieve

    base_dir = Path(__file__).parent.resolve()

    jar_dir = base_dir / "spark_jars"
    jar_dir.mkdir(exist_ok=True)
    os.environ["PD2_JAR_DIR"] = str(jar_dir)

    data_dir = base_dir / "data"
    env_paths = {
        "PD2_DATA_PATH": data_dir,
        "PD2_CLEAN_PATH": data_dir / "clean",
        "PD2_MERGED_PATH": data_dir / "merged",
        "PD2_AGG_PATH": data_dir / "prepared_for_model",
    }

    for key, path in env_paths.items():
        os.environ[key] = str(path)

    MAVEN = "https://repo1.maven.org/maven2"
    jars = {
        "hadoop-aws-3.4.1.jar": f"{MAVEN}/org/apache/hadoop/hadoop-aws/3.4.1/",
        "wildfly-openssl-1.1.3.Final.jar": f"{MAVEN}/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/",
        "bundle-2.24.6.jar": f"{MAVEN}/software/amazon/awssdk/bundle/2.24.6/"
    }

    for filename, url in jars.items():
        dest = jar_dir / filename
        if not dest.exists():
            print(f"Downloading {filename}...")
            urlretrieve(f"{url}{filename}", dest)
