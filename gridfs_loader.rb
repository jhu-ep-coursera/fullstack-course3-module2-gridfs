require 'mongo';
Mongo::Logger.logger.level = ::Logger::DEBUG

class GridfsLoader
  def self.create_connection(mongo_url=nil, db_name=nil)
    mongo_url ||= "mongodb://localhost:27017"
    db_name ||= "test"
    STDERR.puts "creating connection #{mongo_url} #{db_name}"
    db = Mongo::Client.new(mongo_url)
    db.use(db_name)
  end
  def self.mongo_client(mongo_url=nil, db_name=nil)
    @@db ||= create_connection(mongo_url, db_name)
  end

  def initialize(mongo_url=nil, db_name=nil)
    self.class.mongo_connection
  end

  def import_grid_file(file_path, name=nil, contentType=nil, metadata=nil)
    os_file=File.open(file_path)
    description = {}
    description[:filename]=name       if !name.nil?
    description[:contentType]=name    if !contentType.nil?
    description[:metadata] = metadata if !metadata.nil?

    grid_file = Mongo::Grid::File.new(os_file.read, description )
    @@db.database.fs.insert_one(grid_file)
  end

  def find_grid_file(description) 
    @@db.database.fs.find_one(description)
  end

  def export_grid_file(grid_file, file_path) 
    os_file=File.open(file_path,'w')
    grid_file.chunks.reduce([]) { |x,chunk| os_file << chunk.data.data }
  end

  def delete_grid_file(grid_file)
    @@db.database.fs.delete_one(grid_file)
  end
end

