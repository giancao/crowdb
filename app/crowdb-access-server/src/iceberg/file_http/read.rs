use crowdb_access_iceberg::file::{resolve_range, ByteRange};
use hyper::{
    body::Incoming,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, RANGE},
    Method, Request, Response, StatusCode,
};

use super::{
    admission_error, catalog_error, etag, range_error, set_header, FileHttp, FileRequest, FileS3ErrorCode,
    FileTransferAdmission, IcebergBody,
};
use crate::iceberg::file_read_conditions::{file_read_condition, FileReadCondition};

impl FileHttp {
    pub(super) async fn read(
        &self,
        file_request: &FileRequest,
        request: &Request<Incoming>,
        context: crowdb_access_iceberg::catalog::CatalogContext,
        admission: &FileTransferAdmission,
    ) -> Result<Response<IcebergBody>, FileS3ErrorCode> {
        let record = self
            .repository
            .load(context, &file_request.location)
            .await
            .map_err(catalog_error)?
            .ok_or(FileS3ErrorCode::NoSuchKey)?;
        admission.check_bytes(0, record.length).map_err(admission_error)?;
        let condition = file_read_condition(request.headers(), &etag(&record))?;
        let FileReadCondition::Read { use_range } = condition else {
            let mut response = Response::new(IcebergBody::new(Vec::new()));
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            set_header(&mut response, ETAG, &etag(&record))?;
            return Ok(response);
        };
        let range = requested_range(request, record.length, use_range)?;
        let bytes = range.map_or(record.length, |range| range.end - range.start);
        let body = if request.method() == Method::HEAD {
            admission.check_bytes(0, record.length).map_err(admission_error)?;
            IcebergBody::new(Vec::new())
        } else {
            let body = admission
                .read_body(&self.responses, self.blocks.clone(), record.clone(), range)
                .map_err(admission_error)?;
            IcebergBody::file(body)
        };
        let mut response = Response::new(body);
        *response.status_mut() = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        set_header(&mut response, CONTENT_LENGTH, &bytes.to_string())?;
        set_header(&mut response, ETAG, &etag(&record))?;
        response
            .headers_mut()
            .insert(ACCEPT_RANGES, hyper::header::HeaderValue::from_static("bytes"));
        if let Some(range) = range {
            set_header(
                &mut response,
                CONTENT_RANGE,
                &format!("bytes {}-{}/{}", range.start, range.end - 1, record.length),
            )?;
        }
        Ok(response)
    }
}

fn requested_range(
    request: &Request<Incoming>,
    length: u64,
    use_range: bool,
) -> Result<Option<ByteRange>, FileS3ErrorCode> {
    let range = match request
        .headers()
        .get_all(RANGE)
        .iter()
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => None,
        [value] => Some(value.to_str().map_err(|_| FileS3ErrorCode::InvalidRange)?),
        _ => return Err(FileS3ErrorCode::InvalidRange),
    };
    resolve_range(if use_range { range } else { None }, length).map_err(range_error)
}
