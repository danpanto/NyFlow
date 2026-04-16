if __name__ == "__main__":
    from pipeline.app import Pipeline
    from setup import setenv

    setenv()
    Pipeline().run()
