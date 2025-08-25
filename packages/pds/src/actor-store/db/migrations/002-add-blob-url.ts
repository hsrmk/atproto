import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema.alterTable('blob').addColumn('url', 'varchar').execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.alterTable('blob').dropColumn('url').execute()
}
