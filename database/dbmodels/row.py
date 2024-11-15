

class Row:

    def to_dict(self):
        return self.__dict__

    @classmethod
    def table_name(cls):
        raise NotImplementedError

    @classmethod
    def exists(cls):
        return f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '{cls.table_name()}')"

    @classmethod
    def get_len(cls):
        return f"SELECT COUNT(*) AS len FROM {cls.table_name()}"
