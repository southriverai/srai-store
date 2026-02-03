from mailau_server.mailau_container import MailauContainer
from mailau_server.models.background_task import BackgroundTask
import uuid

def test_task_store():
    milau_container = MailauContainer.get_instance()
    task_store = milau_container.background_task_store
    task_id = str(uuid.uuid4())
    task_store.mset([(task_id, BackgroundTask(task_id=task_id, task_type="test", task_status="test", task_error="test"))])
    task_store.mget([task_id])
    task_store.mdelete([task_id])
    task_store.get_all()

if __name__ == "__main__":
    milau_container = MailauContainer.initialize()
    test_task_store()