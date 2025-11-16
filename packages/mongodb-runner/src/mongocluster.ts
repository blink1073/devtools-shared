import type { MongoServerOptions } from './mongoserver';
import { MongoServer } from './mongoserver';
import { ConnectionString } from 'mongodb-connection-string-url';
import type { DownloadOptions } from '@mongodb-js/mongodb-downloader';
import { downloadMongoDb } from '@mongodb-js/mongodb-downloader';
import type { MongoClientOptions } from 'mongodb';
import { MongoClient } from 'mongodb';
import { sleep, range, uuid, debug } from './util';
import { OIDCMockProviderProcess } from './oidc';
import { option } from 'yargs';

export interface RSMemberOptions {
  args?: string[];
  priority?: number;
  tags?: { [key: string]: string };
}
export interface RSOptions {
  arbiters?: number;
  secondaries?: number;
  memberOptions?: RSMemberOptions[];
}

export interface ShardedOptions {
  shards?: number;
  routers?: number;
  configSrvOptions?: RSOptions;
  shardOptions?: RSOptions[];
  routerArgs?: string[][];
}

type CommonClusterOptions = Pick<
  MongoServerOptions,
  | 'logDir'
  | 'tmpDir'
  | 'args'
  | 'binDir'
  | 'docker'
  | 'login'
  | 'password'
  | 'clientOptions'
> & {
  version?: string;
  downloadDir?: string;
  downloadOptions?: DownloadOptions;
  oidc?: string;
  roles?: { [key: string]: string }[];
};

export interface StandaloneClusterOptions extends CommonClusterOptions {
  topology: 'standalone';
}

export interface ReplSetClusterOptions extends CommonClusterOptions, RSOptions {
  topology: 'replset';
}

export interface ShardedClusterOptions
  extends CommonClusterOptions,
    ShardedOptions {
  topology: 'sharded';
}

export type MongoClusterOptions =
  | StandaloneClusterOptions
  | ReplSetClusterOptions
  | ShardedClusterOptions;

export class MongoCluster {
  private topology: MongoClusterOptions['topology'] = 'standalone';
  private replSetName?: string;
  private servers: MongoServer[] = []; // mongod/mongos
  private shards: MongoCluster[] = []; // replsets
  private oidcMockProviderProcess?: OIDCMockProviderProcess;

  private constructor() {
    /* see .start() */
  }

  private static downloadMongoDb(
    tmpdir: string,
    targetVersionSemverSpecifier?: string | undefined,
    options?: DownloadOptions | undefined,
  ): Promise<string> {
    return downloadMongoDb({
      directory: tmpdir,
      version: targetVersionSemverSpecifier,
      downloadOptions: options,
      useLockfile: true,
    });
  }

  serialize(): unknown /* JSON-serializable */ {
    return {
      topology: this.topology,
      replSetName: this.replSetName,
      servers: this.servers.map((srv) => srv.serialize()),
      shards: this.shards.map((shard) => shard.serialize()),
      oidcMockProviderProcess: this.oidcMockProviderProcess?.serialize(),
    };
  }

  isClosed(): boolean {
    return this.servers.length === 0 && this.shards.length === 0;
  }

  static async deserialize(serialized: any): Promise<MongoCluster> {
    const cluster = new MongoCluster();
    cluster.topology = serialized.topology;
    cluster.replSetName = serialized.replSetName;
    cluster.servers = await Promise.all(
      serialized.servers.map((srv: any) => MongoServer.deserialize(srv)),
    );
    cluster.shards = await Promise.all(
      serialized.shards.map((shard: any) => MongoCluster.deserialize(shard)),
    );
    cluster.oidcMockProviderProcess = serialized.oidcMockProviderProcess
      ? OIDCMockProviderProcess.deserialize(serialized.oidcMockProviderProcess)
      : undefined;
    return cluster;
  }

  get hostport(): string {
    return this.servers.map((srv) => srv.hostport).join(',');
  }

  get connectionString(): string {
    const cs = new ConnectionString(`mongodb://${this.hostport}/`);
    if (this.replSetName)
      cs.typedSearchParams<MongoClientOptions>().set(
        'replicaSet',
        this.replSetName,
      );
    return cs.toString();
  }

  get oidcIssuer(): string | undefined {
    return this.oidcMockProviderProcess?.issuer;
  }

  get connectionStringUrl(): ConnectionString {
    return new ConnectionString(this.connectionString);
  }

  get serverVersion(): string {
    return this.servers[0].serverVersion;
  }

  get serverVariant(): 'enterprise' | 'community' {
    return this.servers[0].serverVariant;
  }

  static async start({
    ...options
  }: MongoClusterOptions): Promise<MongoCluster> {
    const cluster = new MongoCluster();
    cluster.topology = options.topology;
    if (!options.binDir) {
      options.binDir = await this.downloadMongoDb(
        options.downloadDir ?? options.tmpDir,
        options.version,
        options.downloadOptions,
      );
    }

    if (options.oidc !== undefined) {
      cluster.oidcMockProviderProcess = await OIDCMockProviderProcess.start(
        options.oidc || '--port=0',
      );
      const oidcServerConfig = [
        {
          issuer: cluster.oidcMockProviderProcess.issuer,
          audience: cluster.oidcMockProviderProcess.audience,
          authNamePrefix: 'dev',
          clientId: 'cid',
          authorizationClaim: 'groups',
        },
      ];
      delete options.oidc;
      options.args = [
        ...(options.args ?? []),
        '--setParameter',
        `oidcIdentityProviders=${JSON.stringify(oidcServerConfig)}`,
        '--setParameter',
        'authenticationMechanisms=SCRAM-SHA-256,MONGODB-OIDC',
        '--setParameter',
        'enableTestCommands=true',
      ];
    }

    if (options.topology === 'standalone') {
      cluster.servers.push(
        await MongoServer.start({
          ...options,
          binary: 'mongod',
        }),
      );
      if (options.login) {
        await cluster.servers[0].addAdminUser(options.roles);
        await cluster.servers[0].reinitialize();
      }
    } else if (options.topology === 'replset') {
      const { secondaries = 2, arbiters = 0 } = options;

      const args = [...(options.args ?? [])];
      let replSetName: string;
      if (!args.includes('--replSet')) {
        replSetName = `replSet-${uuid()}`;
        args.push('--replSet', replSetName);
      } else {
        replSetName = args[args.indexOf('--replSet') + 1];
      }

      const primaryArgs = [...args];
      const memberOptions = options.memberOptions || [{}];
      if (memberOptions.length > 0) {
        primaryArgs.push(...(memberOptions[0].args || []));
      }
      debug('Starting primary', primaryArgs);
      const primary = await MongoServer.start({
        ...options,
        args: primaryArgs,
        binary: 'mongod',
      });
      cluster.servers.push(primary);

      if (args.includes('--port')) {
        args.splice(args.indexOf('--port') + 1, 1, '0');
      }

      debug('Starting secondaries and arbiters', {
        secondaries,
        arbiters,
        args,
      });
      cluster.servers.push(
        ...(await Promise.all(
          range(secondaries + arbiters).map((i) => {
            const secondaryArgs = [...args];
            if (i + 1 < memberOptions.length) {
              secondaryArgs.push(...(memberOptions[i + 1].args || []));
              debug('Adding secondary args', memberOptions[i + 1].args || []);
            }
            return MongoServer.start({
              ...options,
              args: secondaryArgs,
              binary: 'mongod',
            });
          }),
        )),
      );

      await primary.withClient(async (client) => {
        debug('Running rs.initiate');
        const rsConf = {
          _id: replSetName,
          configsvr: args.includes('--configsvr'),
          members: cluster.servers.map((srv, i) => {
            let options: RSMemberOptions = {};
            if (i < memberOptions.length) {
              options = memberOptions[i];
            }
            let priority = i === 0 ? 1 : 0;
            if (options.priority !== undefined) {
              priority = options.priority;
            }
            return {
              _id: i,
              host: srv.hostport,
              arbiterOnly: i > secondaries,
              priority,
              tags: options.tags || {},
            };
          }),
        };
        await client.db('admin').command({
          replSetInitiate: rsConf,
        });

        for (let i = 0; i < 60; i++) {
          const status = await client.db('admin').command({
            replSetGetStatus: 1,
          });
          if (
            status.members.some((member: any) => member.stateStr === 'PRIMARY')
          ) {
            debug(
              'rs.status indicated primary for replset',
              status.set,
              status.members,
            );
            cluster.replSetName = status.set;
            break;
          }
          debug('rs.status did not include primary, waiting...');
          await sleep(1000);
        }

        // Add auth if needed
        if (options.login) {
          // Sleep to give time for the election to settle.
          await sleep(1000);
          await cluster.servers[0].addAdminUser(options.roles);
          for (const server of cluster.servers) {
            await server.reinitialize();
          }
        }
      });
    } else if (options.topology === 'sharded') {
      const { shards = 3 } = options;
      const shardArgs = [...(options.args ?? [])];
      if (shardArgs.includes('--port')) {
        shardArgs.splice(shardArgs.indexOf('--port') + 1, 1, '0');
      }
      const allShardOptions = options.shardOptions || [{}];

      debug('starting config server and shard servers', shardArgs);
      const [configsvr, ...shardsvrs] = await Promise.all(
        range(shards + 1).map((i) => {
          const args: string[] = [...shardArgs];
          let optionsSource: RSOptions = {};
          if (i === 0) {
            args.push('--configsvr');
            optionsSource = options.configSrvOptions || {};
          } else {
            args.push('--shardsvr');
            if (i - 1 < allShardOptions.length) {
              optionsSource = allShardOptions[i - 1];
            }
          }
          return MongoCluster.start({
            ...options,
            ...optionsSource,
            args,
            topology: 'replset',
          });
        }),
      );
      cluster.shards.push(configsvr, ...shardsvrs);

      const routerArgs = options.routerArgs ?? [[]];
      const { routers = 1 } = options;
      for (let i = 0; i < routers; i++) {
        debug('starting mongos', i);
        const args = [...(options.args ?? [])];
        if (routerArgs.length > i - 1) {
          args.push(...routerArgs[i]);
        }
        const mongos = await MongoServer.start({
          ...options,
          binary: 'mongos',
          args: [
            ...args,
            '--configdb',
            `${configsvr.replSetName!}/${configsvr.hostport}`,
          ],
        });
        cluster.servers.push(mongos);
        if (options.login) {
          await mongos.reinitialize();
        }
        await mongos.withClient(async (client) => {
          for (const shard of shardsvrs) {
            const shardSpec = `${shard.replSetName!}/${shard.hostport}`;
            debug('adding shard', shardSpec);
            await client.db('admin').command({
              addShard: shardSpec,
            });
          }
          debug('added shards');
        });
      }
    }
    return cluster;
  }

  async close(): Promise<void> {
    await Promise.all(
      [...this.servers, ...this.shards, this.oidcMockProviderProcess].map(
        (closable) => closable?.close(),
      ),
    );
    this.servers = [];
    this.shards = [];
  }

  async withClient<Fn extends (client: MongoClient) => any>(
    fn: Fn,
    clientOptions: MongoClientOptions = {},
  ): Promise<ReturnType<Fn>> {
    const client = await MongoClient.connect(
      this.connectionString,
      clientOptions,
    );
    try {
      return await fn(client);
    } finally {
      await client.close(true);
    }
  }

  ref(): void {
    for (const child of [...this.servers, ...this.shards]) child.ref();
  }

  unref(): void {
    for (const child of [...this.servers, ...this.shards]) child.unref();
  }
}
