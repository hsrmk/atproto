import stream, { Readable } from 'node:stream'
import { CID } from 'multiformats/cid'
import { BlobNotFoundError, BlobStore } from '@atproto/repo'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { countAll, countDistinct, notSoftDeletedClause } from '../../db/util'
import { StatusAttr } from '../../lexicon/types/com/atproto/admin/defs'
import { ActorDb } from '../db'

export class BlobReader {
  constructor(
    public db: ActorDb,
    public blobstore: BlobStore,
  ) {}

  async getBlobMetadata(
    cid: CID,
  ): Promise<{ size: number; mimeType?: string }> {
    const { ref } = this.db.db.dynamic
    const found = await this.db.db
      .selectFrom('blob')
      .selectAll()
      .where('blob.cid', '=', cid.toString())
      .where(notSoftDeletedClause(ref('blob')))
      .executeTakeFirst()
    if (!found) {
      throw new InvalidRequestError('Blob not found')
    }
    return {
      size: found.size,
      mimeType: found.mimeType,
    }
  }

  async getBlobMetadataWithUrl(
    cid: CID,
  ): Promise<{ size: number; mimeType?: string; url?: string }> {
    const { ref } = this.db.db.dynamic
    const found = await this.db.db
      .selectFrom('blob')
      .selectAll()
      .where('blob.cid', '=', cid.toString())
      .where(notSoftDeletedClause(ref('blob')))
      .executeTakeFirst()
    if (!found) {
      throw new InvalidRequestError('Blob not found')
    }
    return {
      size: found.size,
      mimeType: found.mimeType,
      url: found.url ?? undefined,
    }
  }

  async streamFromUrl(url: string): Promise<stream.Readable> {
    // Use fetch to get the response body as a stream
    // Note: globalThis.fetch is available in Node 18+
    const res = await fetch(url)
    if (!res.ok || !res.body) {
      throw new InvalidRequestError(`Failed to fetch blob from url: ${url}`)
    }
    // Use Readable.fromWeb from node:stream to convert the web ReadableStream to a Node.js Readable
    return Readable.fromWeb(res.body)
  }

  async getBlob(
    cid: CID,
  ): Promise<{ size: number; mimeType?: string; stream: stream.Readable }> {
    let metadataWithUrl = await this.getBlobMetadataWithUrl(cid)
    let blobStream

    if (metadataWithUrl.url) {
      try {
        blobStream = await this.streamFromUrl(metadataWithUrl.url)
      } catch (err) {
        if (err instanceof BlobNotFoundError) {
          throw new InvalidRequestError('Blob not found')
        }
        throw err
      }
    } else {
      metadataWithUrl = await this.getBlobMetadata(cid)

      try {
        blobStream = await this.blobstore.getStream(cid)
      } catch (err) {
        if (err instanceof BlobNotFoundError) {
          throw new InvalidRequestError('Blob not found')
        }
        throw err
      }
    }

    return {
      size: metadataWithUrl.size,
      mimeType: metadataWithUrl.mimeType,
      stream: blobStream,
    }
  }

  async listBlobs(opts: {
    since?: string
    cursor?: string
    limit: number
  }): Promise<string[]> {
    const { since, cursor, limit } = opts
    let builder = this.db.db
      .selectFrom('record_blob')
      .select('blobCid')
      .orderBy('blobCid', 'asc')
      .groupBy('blobCid')
      .limit(limit)
    if (since) {
      builder = builder
        .innerJoin('record', 'record.uri', 'record_blob.recordUri')
        .where('record.repoRev', '>', since)
    }
    if (cursor) {
      builder = builder.where('blobCid', '>', cursor)
    }
    const res = await builder.execute()
    return res.map((row) => row.blobCid)
  }

  async getBlobTakedownStatus(cid: CID): Promise<StatusAttr | null> {
    const res = await this.db.db
      .selectFrom('blob')
      .select('takedownRef')
      .where('cid', '=', cid.toString())
      .executeTakeFirst()
    if (!res) return null
    return res.takedownRef
      ? { applied: true, ref: res.takedownRef }
      : { applied: false }
  }

  async getRecordsForBlob(cid: CID): Promise<string[]> {
    const res = await this.db.db
      .selectFrom('record_blob')
      .where('blobCid', '=', cid.toString())
      .selectAll()
      .execute()
    return res.map((row) => row.recordUri)
  }

  async getBlobsForRecord(recordUri: string): Promise<string[]> {
    const res = await this.db.db
      .selectFrom('blob')
      .innerJoin('record_blob', 'record_blob.blobCid', 'blob.cid')
      .where('recordUri', '=', recordUri)
      .select('blob.cid')
      .execute()
    return res.map((row) => row.cid)
  }

  async blobCount(): Promise<number> {
    const res = await this.db.db
      .selectFrom('blob')
      .select(countAll.as('count'))
      .executeTakeFirst()
    return res?.count ?? 0
  }

  async recordBlobCount(): Promise<number> {
    const { ref } = this.db.db.dynamic
    const res = await this.db.db
      .selectFrom('record_blob')
      .select(countDistinct(ref('blobCid')).as('count'))
      .executeTakeFirst()
    return res?.count ?? 0
  }

  async listMissingBlobs(opts: {
    cursor?: string
    limit: number
  }): Promise<{ cid: string; recordUri: string }[]> {
    const { cursor, limit } = opts
    let builder = this.db.db
      .selectFrom('record_blob')
      .whereNotExists((qb) =>
        qb
          .selectFrom('blob')
          .selectAll()
          .whereRef('blob.cid', '=', 'record_blob.blobCid'),
      )
      .selectAll()
      .orderBy('blobCid', 'asc')
      .groupBy('blobCid')
      .limit(limit)
    if (cursor) {
      builder = builder.where('blobCid', '>', cursor)
    }
    const res = await builder.execute()
    return res.map((row) => ({
      cid: row.blobCid,
      recordUri: row.recordUri,
    }))
  }

  async getBlobCids() {
    const blobRows = await this.db.db.selectFrom('blob').select('cid').execute()
    return blobRows.map((row) => CID.parse(row.cid))
  }
}
