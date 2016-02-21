require 'mongo';
Mongo::Logger.logger.level = ::Logger::DEBUG

class GridfsLoader
  # performs the detailed work to create a new MongoDB connection
  def self.create_connection(mongo_url=nil, db_name=nil)
    mongo_url ||= "mongodb://localhost:27017"
    db_name ||= "test"
    STDERR.puts "creating connection #{mongo_url} #{db_name}"
    db = Mongo::Client.new(mongo_url)
    db.use(db_name)
  end

  #creates and/or returns a MongoDB connection cached in the class
  def self.mongo_client(mongo_url=nil, db_name=nil)
    @@db ||= create_connection(mongo_url, db_name)
  end

  #sets up the object instance with a MongoDB connection
  def initialize(mongo_url=nil, db_name=nil)
    @@db = self.class.mongo_client(mongo_url=nil, db_name=nil)
  end

  # reads the contents of the file and inserts into GridFS along
  # with some optional metadata. The :_id of the file is returned.
  def import_grid_file(file_path, name=nil, contentType=nil, metadata=nil)
    os_file=File.open(file_path,'rb')
    description = {}
    description[:filename]=name       if !name.nil?
    description[:contentType]=name    if !contentType.nil?
    description[:metadata] = metadata if !metadata.nil?

    grid_file = Mongo::Grid::File.new(os_file.read, description )
    @@db.database.fs.insert_one(grid_file)
  end

  # locates a file in GridFS based on a criteria. Use {} if you 
  # don't care which file you get. Use :_id=>id if you have the 
  # id of the file
  def find_grid_file(criteria) 
    @@db.database.fs.find_one(criteria)
  end

  # provide the grid_file object (from find_grid_file) and a path
  # to write the data to
  def export_grid_file(grid_file, file_path) 
    os_file=File.open(file_path,'wb')
    grid_file.chunks.reduce([]) { |x,chunk| os_file << chunk.data.data }
  end

  # provide the grid_file object (from find_grid_file) to delete 
  # that file
  def delete_grid_file(grid_file)
    @@db.database.fs.delete_one(grid_file)
  end
end

