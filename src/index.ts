import { Readable, ReadableStateOptions } from 'readable-stream'
import S3, {
  ListObjectsV2Request,
  ListObjectsV2Output,
} from 'aws-sdk/clients/s3'

type S3ListBucketStreamOptions = ReadableStateOptions & {
  // If true the stream will emit full Metadata objects for every listed bucket
  // object. Default `false`
  fullMetadata?: boolean,
}

const defaultOptions = {
  fullMetadata: false,
}

const defaultListObjectsV2Options: Partial<ListObjectsV2Request> = {
  MaxKeys: 1000,
}

// Readable stream that lists all the objects form an S3 bucket recursively
class S3ListBucketStream extends Readable {
  private _s3: S3
  private _bucket: string
  private _bucketPrefix: string
  private _fullMetadata: boolean
  private _listObjectsV2args: Partial<ListObjectsV2Request>
  private _lastResponse: ListObjectsV2Output
  private _currentIndex: number
  private _stopped: boolean

  constructor (
    // An S3 client from the AWS SDK (or any object that implements a
    // compatible `listObjectsV2` method)
    s3: S3,
    // The name of the bucket to list
    bucket: string,
    // An optional prefix to list only files with the given prefix
    bucketPrefix = '',
    // Stream options
    options: S3ListBucketStreamOptions = {},
    // Extra arguments to be passed to the listObjectsV2 call in the S3 client
    listObjectsV2args: Partial<ListObjectsV2Request> = {},
  ) {
    const mergedOptions = { ...defaultOptions, ...options }

    // forces object mode if full metadata is enabled
    if (mergedOptions.fullMetadata) {
      mergedOptions.objectMode = true
    }

    // invoke parent constructor
    super(mergedOptions)

    // config
    this._s3 = s3
    this._bucket = bucket
    this._bucketPrefix = bucketPrefix
    this._fullMetadata = mergedOptions.fullMetadata
    this._listObjectsV2args = {
      ...defaultListObjectsV2Options,
      ...listObjectsV2args,
    }

    // internal state
    this._lastResponse = undefined
    this._currentIndex = undefined
    this._stopped = true
  }

  _loadNextPage () {
    return new Promise((resolve, reject) => {
      const currentParams = {
        Bucket: this._bucket,
        Prefix: this._bucketPrefix,
        ContinuationToken: this._lastResponse
          ? this._lastResponse.NextContinuationToken
          : undefined,
      }

      const params = Object.assign({}, this._listObjectsV2args, currentParams)

      this._s3.listObjectsV2(params, (err, data) => {
        if (err) {
          return reject(err)
        }

        this._lastResponse = data
        this._currentIndex = 0

        this.emit('page', { params, data })
        return resolve()
      })
    })
  }

  async _startRead () {
    try {
      this._stopped = false
      this.emit('restarted', true)
      while (true) {
        if (
          typeof this._lastResponse === 'undefined' ||
          this._currentIndex >= this._lastResponse.Contents.length
        ) {
          if (this._lastResponse && !this._lastResponse.IsTruncated) {
            const result = this.push(null) // stream is over
            return result
          }
          await this._loadNextPage()
        }

        const metadata = this._lastResponse.Contents[this._currentIndex++]
        const chunkToPush = this._fullMetadata ? metadata : metadata.Key // return only the object key (file name)

        if (!this.push(chunkToPush)) {
          this._stopped = true
          this.emit('stopped', true)
          break // reader buffer full, stop until next _read call
        }
      }
    } catch (err) {
      return this.emit('error', err)
    }
  }

  _read (size: number) {
    if (this._stopped) {
      // if stopped, restart reading from the source S3 api
      this._startRead()
    }
  }
}

export default S3ListBucketStream
