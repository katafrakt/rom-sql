require "rom/configuration"

class TestConfiguration < ROM::Configuration
  def relation(name, *, &block)
    if registered_relation_names.include?(name)
      setup.components.relations.delete_if do |component|
        component.id == name
      end
    end
    super
  end

  def registered_relation_names
    setup.components.relations.map(&:id)
  end
end
