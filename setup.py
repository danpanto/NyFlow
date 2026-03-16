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
        "PD2_JAR_DIR": jar_dir,
        "PD2_DATA_DIR": data_dir,
        "PD2_CLEAN_DIR": data_dir / "clean",
        "PD2_MERGED_DIR": data_dir / "merged",
        "PD2_AGG_DIR": data_dir / "agg",
    }

    for key, path in env_paths.items():
        os.environ[key] = str(path)

    MAVEN = "https://repo1.maven.org/maven2"
    AZURE_MAVEN = "https://mmlspark.azureedge.net/maven"
    jars = {
        "hadoop-aws-3.4.1.jar":                     f"{MAVEN}/org/apache/hadoop/hadoop-aws/3.4.1/",
        "wildfly-openssl-1.1.3.Final.jar":          f"{MAVEN}/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/",
        "bundle-2.24.6.jar":                        f"{MAVEN}/software/amazon/awssdk/bundle/2.24.6/",
        "synapseml_2.12-1.1.2.jar":                 f"{AZURE_MAVEN}/com/microsoft/azure/synapseml_2.12/1.1.2/",
        "synapseml-core_2.12-1.1.2.jar":            f"{MAVEN}/com/microsoft/azure/synapseml-core_2.12/1.1.2/",
        # "synapseml-deep-learning_2.12-1.1.2.jar":   f"{MAVEN}/com/microsoft/azure/synapseml-deep-learning_2.12/1.1.2/"
    }

    for filename, url in jars.items():
        dest = jar_dir / filename
        if not dest.exists():
            print(f"Downloading {filename}...")
            urlretrieve(f"{url}{filename}", dest)
