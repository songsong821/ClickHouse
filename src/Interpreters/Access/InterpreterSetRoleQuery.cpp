#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterSetRoleQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SET_NON_GRANTED_ROLE;
}


BlockIO InterpreterSetRoleQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query_ptr->as<const ASTSetRoleQuery &>();

    if (query.kind == ASTSetRoleQuery::Kind::SET_DEFAULT_ROLE)
        return setDefaultRole(updated_query_ptr, query);

    setRole(query);
    return {};
}


void InterpreterSetRoleQuery::setRole(const ASTSetRoleQuery & query)
{
    auto session_context = getContext()->getSessionContext();

    if (query.kind == ASTSetRoleQuery::Kind::SET_ROLE_DEFAULT)
        session_context->setCurrentRolesDefault();
    else
        session_context->setCurrentRoles(RolesOrUsersSet{*query.roles, session_context->getAccessControl()});
}


BlockIO InterpreterSetRoleQuery::setDefaultRole(const ASTPtr & updated_query_ptr, const ASTSetRoleQuery & query)
{
    /// `CURRENT_USER` must be resolved here, on the initiator: the AST text is what reaches every host, and a
    /// DDL worker there runs as its own user, so an unresolved tag would set the default roles of the wrong user.
    query.replaceCurrentUserTag(getContext()->getUserName());

    getContext()->getAccess()->checkCanAdministerDefaultRoles();
    getContext()->checkAccess(query.to_users->collectRequiredGrants(AccessType::ALTER_USER));

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    auto & access_control = getContext()->getAccessControl();
    std::vector<UUID> to_users = RolesOrUsersSet{*query.to_users, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);
    RolesOrUsersSet roles_from_query{*query.roles, access_control};

    auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
    {
        auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
        updateUserSetDefaultRoles(*updated_user, roles_from_query);
        return updated_user;
    };

    access_control.update(to_users, update_func);
    return {};
}


void InterpreterSetRoleQuery::updateUserSetDefaultRoles(User & user, const RolesOrUsersSet & roles_from_query)
{
    if (!roles_from_query.all)
    {
        for (const auto & id : roles_from_query.getMatchingIDs())
        {
            if (!user.granted_roles.isGranted(id))
                throw Exception(ErrorCodes::SET_NON_GRANTED_ROLE, "Role should be granted to set default");
        }
    }
    user.default_roles = roles_from_query;
}

void registerInterpreterSetRoleQuery(InterpreterFactory & factory);
void registerInterpreterSetRoleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSetRoleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterSetRoleQuery", create_fn);
}

}
