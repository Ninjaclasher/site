import json
import logging
import struct
from collections import namedtuple

from judge.bridge.base_handler import Disconnect, ZlibPacketHandler

logger = logging.getLogger('judge.bridge')
size_pack = struct.Struct('!I')


SubmissionJudgeRequest = namedtuple('SubmissionJudgeRequest', 'id problem language source judge_id priority')


class DjangoHandler(ZlibPacketHandler):
    def __init__(self, request, client_address, server, judges):
        super().__init__(request, client_address, server)

        self.handlers = {
            'submission-request': self.on_submission,
            'terminate-submission': self.on_termination,
            'disconnect-judge': self.on_disconnect_request,
        }
        self.judges = judges

    def send(self, data):
        super().send(json.dumps(data, separators=(',', ':')))

    def on_packet(self, packet):
        packet = json.loads(packet)
        try:
            result = self.handlers.get(packet.get('name', None), self.on_malformed)(packet)
        except Exception:
            logger.exception('Error in packet handling (Django-facing)')
            result = {'name': 'bad-request'}
        self.send(result)
        raise Disconnect()

    def on_submission(self, data):
        judge_id = data['judge-id']
        priority = data['priority']
        if not self.judges.check_priority(priority):
            return {'name': 'bad-request'}

        submissions = [
            SubmissionJudgeRequest(
                id=sub['submission-id'],
                problem=sub['problem-id'],
                language=sub['language'],
                source=sub['source'],
                judge_id=judge_id,
                priority=priority,
            ) for sub in data['submissions']
        ]
        self.judges.judge(submissions)
        return {'name': 'submission-received', 'submission-count': len(submissions)}

    def on_termination(self, data):
        return {'name': 'submission-received', 'judge-aborted': self.judges.abort(data['submission-id'])}

    def on_disconnect_request(self, data):
        judge_id = data['judge-id']
        force = data['force']
        self.judges.disconnect(judge_id, force=force)

    def on_malformed(self, packet):
        logger.error('Malformed packet: %s', packet)

    def on_close(self):
        self._to_kill = False
