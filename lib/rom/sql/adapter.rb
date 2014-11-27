require 'logger'

require 'rom/sql/commands'

module ROM
  module SQL

    class Adapter < ROM::Adapter
      attr_reader :connection

      def self.schemes
        [:ado, :amalgalite, :cubrid, :db2, :dbi, :do, :fdbsql, :firebird, :ibmdb,
         :informix, :jdbc, :mysql, :mysql2, :odbc, :openbase, :oracle, :postgres,
         :sqlanywhere, :sqlite, :swift, :tinytds]
      end

      def initialize(*args)
        super
        @connection = ::Sequel.connect(uri.to_s)
      end

      def [](name)
        connection[name]
      end

      def command(name, relation, definition)
        case name
        when :create then Commands::Create.build(relation, definition)
        when :update then Commands::Update.build(relation, definition)
        when :delete then Commands::Delete.build(relation)
        else
          raise ArgumentError, "#{name} is not a supported command"
        end
      end

      def schema
        tables.map { |table| [table, dataset(table), columns(table)] }
      end

      def extend_relation_class(klass)
        klass.send(:include, RelationInclusion)
      end

      def extend_relation_instance(relation)
        relation.extend(RelationExtension)
      end

      private

      def tables
        connection.tables
      end

      def columns(table)
        dataset(table).columns
      end

      def dataset(table)
        connection[table]
      end

      def attributes(table)
        map_attribute_types connection.schema(table)
      end

      def map_attribute_types(attrs)
        attrs.map do |column, opts|
          [column, { type: map_schema_type(opts[:type]) }]
        end.to_h
      end

      def map_schema_type(type)
        connection.class::SCHEMA_TYPE_CLASSES.fetch(type)
      end

      ROM::Adapter.register(self)
    end

  end
end
