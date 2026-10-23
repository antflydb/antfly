import io
import json
import unittest

from qualify_native_sse_abort import first_content_delta


def event(delta=None,finish=None,usage=None):
    value={'choices':[{'index':0,'delta':delta or {},'finish_reason':finish}]}
    if usage is not None:value['usage']=usage
    return ('data: '+json.dumps(value)+'\n\n').encode()


class NativeSseAbortEvidenceTests(unittest.TestCase):
    def test_stops_at_actual_content_without_consuming_terminal_events(self):
        tail=event({'content':'later'})+b'data: [DONE]\n\n'
        response=io.BytesIO(event({'role':'assistant'})+event({'content':'first'})+tail)
        result=first_content_delta(response)
        self.assertEqual(result['content'],'first')
        self.assertEqual(response.read(),b'\n'+tail)

    def test_completed_or_role_only_stream_cannot_qualify(self):
        for data in [event({'role':'assistant'}),b'data: [DONE]\n\n',
                     event({'content':'already complete'},finish='stop')]:
            with self.subTest(data=data),self.assertRaises(AssertionError):
                first_content_delta(io.BytesIO(data))

    def test_error_and_usage_cannot_count_as_content(self):
        for data in [b'event: error\ndata: failure\n\n',
                     event({'content':'last'},usage={'completion_tokens':512})]:
            with self.subTest(data=data),self.assertRaises(AssertionError):
                first_content_delta(io.BytesIO(data))

    def test_limits_line_size_and_empty_events(self):
        for data in [b'x'*65537+b'\n',b'\n'*256]:
            with self.subTest(size=len(data)),self.assertRaises(AssertionError):
                first_content_delta(io.BytesIO(data))


if __name__=='__main__':unittest.main()
