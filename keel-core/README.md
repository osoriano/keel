# keel-core

Core model interfaces and classes, intended to be shared across the code base.

Please avoid adding other Keel modules as dependencies to this project, which leads to circular dependencies.
A common case is creating Spring components in this module that depend on components from other modules. Don't do it. :)
