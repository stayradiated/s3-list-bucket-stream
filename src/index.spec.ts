import test from 'ava'
import S3, {
  ListObjectsV2Request,
  ListObjectsV2Output,
} from 'aws-sdk/clients/s3'

import S3ListBucketStream from './index'

class MockS3 {
  private _mockedPages: ListObjectsV2Output[]
  private _currentPage: number
  private _shouldFail: boolean

  public readonly receivedParams: ListObjectsV2Request[]

  constructor (mockedPages: ListObjectsV2Output[], shouldFail = false) {
    this._mockedPages = mockedPages
    this._currentPage = 0
    this._shouldFail = shouldFail

    this.receivedParams = []
  }

  listObjectsV2 (
    params: ListObjectsV2Request,
    cb: (error: Error, response: ListObjectsV2Output) => void,
  ) {
    this.receivedParams.push(params)
    process.nextTick(() => {
      const error = this._shouldFail ? new Error('some error') : null
      const response = this._shouldFail
        ? null
        : this._mockedPages[this._currentPage++]
      cb(error, response)
    })
  }
}

const createPage = (
  numItems: number,
  startCount = 0,
  last = false,
): ListObjectsV2Output => {
  const items = []
  for (let i = 0; i < numItems; i++) {
    items.push({
      ETag: `etag-${i + startCount}`,
      Key: `${i + startCount}.jpg`,
      LastModified: new Date('2019-02-19T19:35:20.892Z'),
      Size: (i + startCount) * 100,
      StorageClass: 'STANDARD',
    })
  }

  return {
    Contents: items,
    IsTruncated: !last,
    KeyCount: numItems,
    MaxKeys: numItems,
    Name: 'examplebucket',
    NextContinuationToken: last ? undefined : `token-${numItems + startCount}`,
    Prefix: '',
  }
}

test.cb('It should emit all the files from different pages', (t) => {
  const s3 = new MockS3([
    createPage(2),
    createPage(2, 2),
    createPage(2, 4, true),
  ])

  const records = [] as string[]
  let numPages = 0

  const stream = new S3ListBucketStream(
    (s3 as unknown) as S3,
    'some-bucket',
    '/some/prefix',
    {
      MaxKeys: 2,
    },
  )
  stream.on('data', (item) => records.push(item))
  stream.on('error', t.fail)
  stream.on('page', () => numPages++)
  stream.on('end', () => {
    const continuationTokens = s3.receivedParams.map(
      (item) => item.ContinuationToken,
    )

    const maxKeysOptions = s3.receivedParams.map((item) => item.MaxKeys)

    t.snapshot(records)
    t.snapshot(maxKeysOptions)
    t.is(numPages, 3)
    t.snapshot(continuationTokens)
    t.end()
  })
})

test.cb(
  'It should emit all the files from different pages with full metadata',
  (t) => {
    const s3 = new MockS3([
      createPage(2),
      createPage(2, 2),
      createPage(2, 4, true),
    ])

    const records = [] as string[]
    let numPages = 0

    const stream = new S3ListBucketStream(
      (s3 as unknown) as S3,
      'some-bucket',
      '/some/prefix',
    )
    stream.on('data', (item) => records.push(item))
    stream.on('error', t.fail)
    stream.on('page', () => numPages++)
    stream.on('end', () => {
      const continuationTokens = s3.receivedParams.map(
        (item) => item.ContinuationToken,
      )

      t.snapshot(records)
      t.is(numPages, 3)
      t.snapshot(continuationTokens)
      t.end()
    })
  },
)

test.cb(
  'It should emit an error if it fails to make the API call to AWS',
  (t) => {
    const s3 = new MockS3([], true)
    const records = [] as string[]
    const stream = new S3ListBucketStream(
      (s3 as unknown) as S3,
      'some-bucket',
      '/some/prefix',
    )
    stream.on('data', (item) => records.push(item))
    stream.on('error', (err) => {
      t.deepEqual(records, [])
      t.is(err.message, 'some error')
      t.end()
    })
  },
)

test.cb('The stream should pause if reader buffer is full', (t) => {
  const s3 = new MockS3([
    createPage(2),
    createPage(2, 2),
    createPage(2, 4, true),
  ])
  const stream = new S3ListBucketStream(
    (s3 as unknown) as S3,
    'some-bucket',
    '/some/prefix',
  )

  let pushes = 0
  const originalPushFn = stream.push.bind(stream)
  stream.push = function push (data) {
    originalPushFn(data)
    pushes++
    return pushes > 1 // pauses (return false) only the first time
  }

  const records = [] as string[]
  let emittedStopped = false
  let emittedRestarted = false
  stream.on('data', (item) => records.push(item))
  stream.on('stopped', () => (emittedStopped = true))
  stream.on('restarted', () => (emittedRestarted = true))
  stream.on('error', t.fail)
  stream.on('end', () => {
    t.snapshot(records)
    t.true(emittedStopped)
    t.true(emittedRestarted)
    t.end()
  })
})
