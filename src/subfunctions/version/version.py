class Version:
    def __init__(self, base_configs):
        self.base_configs = base_configs

    def show_version(self):
        version = self.base_configs.get('nws', {}).get('version')
        print(f"The application is currently running version {version}.")
