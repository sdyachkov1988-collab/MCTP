"""
WsEventDeduplicator — дедупликация WS событий по event_id (v0.7).
Буфер FIFO ограничен 1000 событиями.
"""
from collections import deque


class WsEventDeduplicator:
    """
    Запоминает event_id последних 1000 событий.
    is_duplicate() возвращает True при повторном event_id и
    НЕ добавляет его в буфер повторно.
    """

    _MAX_BUFFER: int = 1000

    def __init__(self) -> None:
        self._seen:  set[str]       = set()
        self._queue: deque[str]     = deque()

    def is_duplicate(self, event_id: str) -> bool:
        """
        True если event_id уже видели.
        Если не дубликат — добавить в буфер, вытеснив самый старый при переполнении.
        """
        if event_id in self._seen:
            return True
        if len(self._queue) >= self._MAX_BUFFER:
            evicted = self._queue.popleft()
            self._seen.discard(evicted)
        self._seen.add(event_id)
        self._queue.append(event_id)
        return False

    def reset(self) -> None:
        """Очистить буфер."""
        self._seen.clear()
        self._queue.clear()
