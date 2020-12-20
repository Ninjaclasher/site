import json
import logging
import socket
import struct
import zlib

from django.conf import settings
from django.db.models.query import QuerySet

from judge import event_poster as event

logger = logging.getLogger('judge.judgeapi')
size_pack = struct.Struct('!I')


def _post_update_submission(submission, done=False):
    if submission.problem.is_public:
        event.post('submissions', {'type': 'done-submission' if done else 'update-submission',
                                   'id': submission.id,
                                   'contest': submission.contest_key,
                                   'user': submission.user_id, 'problem': submission.problem_id,
                                   'status': submission.status, 'language': submission.language.key})


def judge_request(packet, reply=True):
    sock = socket.create_connection(settings.BRIDGED_DJANGO_CONNECT or
                                    settings.BRIDGED_DJANGO_ADDRESS[0])

    output = json.dumps(packet, separators=(',', ':'))
    output = zlib.compress(output.encode('utf-8'))
    writer = sock.makefile('wb')
    writer.write(size_pack.pack(len(output)))
    writer.write(output)
    writer.close()

    if reply:
        reader = sock.makefile('rb', -1)
        input = reader.read(size_pack.size)
        if not input:
            raise ValueError('Judge did not respond')
        length = size_pack.unpack(input)[0]
        input = reader.read(length)
        if not input:
            raise ValueError('Judge did not respond')
        reader.close()
        sock.close()

        result = json.loads(zlib.decompress(input).decode('utf-8'))
        return result


def judge_submission(submissions, rejudge=False, judge_id=None):
    from .models import Submission, SubmissionTestCase

    CONTEST_SUBMISSION_PRIORITY = 0
    DEFAULT_PRIORITY = 1
    REJUDGE_PRIORITY = 2
    BATCH_REJUDGE_PRIORITY = 3

    if not isinstance(submissions, QuerySet):
        submissions = Submission.objects.filter(id__in=submissions)

    # This should prevent double rejudge issues by permitting only the judging of
    # QU (which is the initial state) and D (which is the final state).
    # Even though the bridge will not queue a submission already being judged,
    # we will destroy the current state by deleting all SubmissionTestCase objects.
    # However, we can't drop the old state immediately before a submission is set for judging,
    # as that would prevent people from knowing a submission is being scheduled for rejudging.
    # It is worth noting that this mechanism does not prevent a new rejudge from being scheduled
    # while already queued, but that does not lead to data corruption.
    submissions.exclude(status__in=('P', 'G'))
    submission_count = submissions.count()

    if not submission_count:
        return False

    batch_rejudge = submission_count > 1

    updates = {'time': None, 'memory': None, 'points': None, 'result': None, 'case_points': 0,
               'case_total': 0, 'error': None, 'was_rejudged': rejudge or batch_rejudge, 'status': 'QU'}

    submissions.update(**updates)
    # This is set proactively; it might get unset in judge_handler's on_grading_begin if the problem doesn't
    # actually have pretests stored on the judge.
    submissions.filter(
        contest_object__run_pretests_only=True,
        contest__problem__is_pretested=True,
    ).update(is_pretested=True)

    # TODO: perhaps we should employ join_sql_subquery if this is too slow
    SubmissionTestCase.objects.filter(submission__in=submissions).delete()

    if batch_rejudge:
        priority = BATCH_REJUDGE_PRIORITY
    elif rejudge:
        priority = REJUDGE_PRIORITY
    # This branch should never be reached when there is more than one submission, so we can
    # simply grab the first submission and check if it is a contest submission to determine the
    # priority.
    elif submissions[0].contest_object_id is not None:
        priority = CONTEST_SUBMISSION_PRIORITY
    else:
        priority = DEFAULT_PRIORITY

    try:
        response = judge_request({
            'name': 'submission-request',
            'submissions': [
                {
                    'submission-id': submission['id'],
                    'problem-id': submission['problem__code'],
                    'language': submission['language__key'],
                    'source': submission['source__source'],
                }
                for submission in submissions.values_list('id', 'problem__code', 'language__key', 'source__source')
            ],
            'judge-id': judge_id,
            'priority': priority,
        })
    except BaseException:
        logger.exception('Failed to send request to judge')
        submissions.update(status='IE', result='IE')
        success = False
    else:
        if response['name'] != 'submission-received' or response['submission-count'] != submission_count:
            submissions.update(status='IE', result='IE')
            success = False
        else:
            success = True
        for submission in submissions.select_related('contest_object', 'language').iterator():
            _post_update_submission(submission)
    return success


def disconnect_judge(judge, force=False):
    judge_request({'name': 'disconnect-judge', 'judge-id': judge.name, 'force': force}, reply=False)


def abort_submission(submission):
    from .models import Submission
    response = judge_request({'name': 'terminate-submission', 'submission-id': submission.id})
    # This defaults to true, so that in the case the JudgeList fails to remove the submission from the queue,
    # and returns a bad-request, the submission is not falsely shown as "Aborted" when it will still be judged.
    if not response.get('judge-aborted', True):
        Submission.objects.filter(id=submission.id).update(status='AB', result='AB', points=0)
        event.post('sub_%s' % Submission.get_id_secret(submission.id), {'type': 'aborted-submission'})
        _post_update_submission(submission, done=True)
