class StageRegistry:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StageRegistry, cls).__new__(cls)
            cls._instance._registry = {}
        return cls._instance

    def register_stage(self, stage_id, stage_cls):
        self._registry[stage_id] = stage_cls

    def get_stage(self, stage_id):
        return self._registry.get(stage_id)

    def get_all_stages(self):
        return self._registry.items()

    def __contains__(self, stage_id):
        return stage_id in self._registry


stage_registry = StageRegistry()
