import csv
import io
import logging
import zipfile

from datetime import datetime
from flask import url_for, request, make_response, send_file
from flask_restful import Resource

from wazo_confd.auth import required_acl
from wazo_confd.helpers.restful import ItemResource, ListResource

from .schema import CDRListRequestSchema, QueueFeaturesSchema, SummarySchema
import json
from .schema import CDRSchema
from wazo_confd_survey.survey.schema import SurveySchema
from wazo_confd_call_note.call_info.schema import CallInfoSchema
from wazo_confd_call_note.tag.schema import TagSchema
from wazo_call_logd.http import AuthResource


logger = logging.getLogger(__name__)

class CDRListResource(AuthResource):
    def __init__(self, service):
        self.service = service
        self.schema = CDRSchema

    @required_acl('confd.call-logd.cdr.read')
    def get(self):
        tenant_uuid = request.headers.get('Wazo-Tenant')
        params = CDRListRequestSchema().load(request.args)
        (cdr_list, count, filtered, count_query, count_query_with_tags, tags) = self.service.get_cdr(tenant_uuid, params)

        tags_list = TagSchema(many = True).dump(tags.all())
        tag_lookup = {str(tag['id']): tag for tag in tags_list}


        summary_data_with_tags = []
        for s in count_query_with_tags.all():
            s=s._asdict()
            s['tag'] = tag_lookup[s['tag_id']] if s['tag_id'] in tag_lookup else None
            summary_data_with_tags.append(s)
        summary = SummarySchema(many=True).dump(count_query.all())
        summary_with_tags = SummarySchema(many=True).dump(summary_data_with_tags)

        output = []
        cdr_schema = CDRSchema()
        survey_schema = SurveySchema()
        callinfo_schema = CallInfoSchema()
        append_output = output.append  # Local binding for faster access

        for calllog, survey, callinfo in cdr_list.all():
            calllog_data = cdr_schema.dump(calllog)
            survey_data = survey_schema.dump(survey) if survey else None
            call_info_data = callinfo_schema.dump(callinfo) if callinfo else None

            if call_info_data:
                tag_ids = call_info_data.pop('tag_ids', '')
                if tag_ids:
                    call_info_data['tags'] = [tag_lookup[tid] for tid in tag_ids.split(',') if tid in tag_lookup]

            append_output({
                **calllog_data,
                'survey': survey_data,
                'call_info': call_info_data,
            })
        if params.get('format') == 'csv':
            mapped_output = self.remap_cdr_data(output, language=params['language'])
            csv = self.create_csv_zip(mapped_output)

            return send_file(
                csv,
                mimetype='application/zip',
                as_attachment=True,
                attachment_filename='workano-cdr.zip'
            )
        return {'count': count, 'filtered': filtered, 'summary': summary,'summary_with_tags':summary_with_tags, 'items': output}
        
    def get_keys(self, language):
        fa_keys = {
            'id': 'آیدی',
            'date': 'تاریخ',
            'source': 'مبدا تماس',
            'source_name': 'نام مبدا',
            'destination': 'مقصد تماس',
            'destination_name': 'نام مقصد',
            'requested_extension': 'داخلی درخواست شده',
            'destination_type': 'نوع مقصد',
            'duration': 'مدت مکالمه',
            'answered': 'پاسخ داده شده',
            'call_direction': 'نوع',
            'tags': 'برچسب‌ها',
            'rate': 'امتیاز کاربر',
        }
        en_keys = {
            'id':'ID',
            'date':'Date',
            'source':'Source',
            'source_name':'Source Name',
            'destination':'Destination',
            'destination_name':'Destination Name',
            'requested_extension':'Requested Extension',
            'destination_type':'Destination Type',
            'duration':'Duration',
            'answered':'Answered',
            'call_direction':'Call Direction',
            'tags':'Tags',
            'rate':'Rate',
        }
        if language == 'fa':
            return fa_keys
        else :
            return en_keys
    def remap_cdr_data(self, cdr_data, language):
        keys = self.get_keys(language)
        return [
            {
                keys['id']: cdr['id'],
                # keys['date']: jdatetime.datetime.fromgregorian(datetime=datetime.fromisoformat(cdr['start'])),
                keys['date']: cdr['start'],
                keys['source']: cdr['source_extension'],
                keys['source_name']: cdr['source_name'],
                keys['destination']: cdr['destination_extension'],
                keys['destination_name']: cdr['destination_name'],
                keys['requested_extension']: cdr['requested_extension'],
                keys['destination_type']: cdr.get('destination_details',{}).get('type'),
                keys['duration']: cdr['duration'],
                keys['answered']: cdr['answered'],
                keys['call_direction']: cdr['call_direction'],
                keys['tags']: ', '.join(tag['name'] for tag in (cdr['call_info'].get('tags', []))) if cdr.get('call_info') else None,
                keys['rate']: cdr['survey'].get('rate', []) if cdr.get('survey') else None,
            }
            for cdr in cdr_data
        ]

    def create_csv_zip(self, output):
            csv_buffer = io.StringIO()
            fieldnames = output[0].keys()
            csv_buffer.write('\ufeff')
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output)

            # 2. Convert CSV string to bytes for zipfile
            csv_bytes = io.BytesIO()
            csv_bytes.write(csv_buffer.getvalue().encode('utf-8'))
            csv_bytes.seek(0)

            # 3. Create ZIP in memory
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr('data.csv', csv_bytes.read())

            zip_buffer.seek(0)
            return zip_buffer
