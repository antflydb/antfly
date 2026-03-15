import { AntflyClient } from "@antfly/sdk";
import { Key, Plus, Shield, Trash2 } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Alert } from "../components/ui/alert";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "../components/ui/dialog";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";
import type { Permission } from "../contexts/auth-context";
import { useApiConfig } from "../hooks/use-api-config";
import { useAuth } from "../hooks/use-auth";

interface UserListItem {
  username: string;
}

export function UsersPage() {
  const { apiUrl } = useApiConfig();
  const { user: currentUser } = useAuth();
  const [users, setUsers] = useState<UserListItem[]>([]);
  const [selectedUser, setSelectedUser] = useState<string | null>(null);
  const [permissions, setPermissions] = useState<Permission[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  // Create user dialog state
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [createError, setCreateError] = useState("");

  // Add permission dialog state
  const [addPermDialogOpen, setAddPermDialogOpen] = useState(false);
  const [newPermResource, setNewPermResource] = useState("");
  const [newPermResourceType, setNewPermResourceType] = useState<"table" | "user" | "*">("table");
  const [newPermType, setNewPermType] = useState<"read" | "write" | "admin">("read");

  // Change password dialog state
  const [changePasswordDialogOpen, setChangePasswordDialogOpen] = useState(false);
  const [newPasswordValue, setNewPasswordValue] = useState("");
  const [passwordChangeError, setPasswordChangeError] = useState("");

  // Create SDK client with stored credentials
  const client = useMemo(() => {
    const stored = localStorage.getItem("antfly_auth");
    if (!stored) {
      return new AntflyClient({ baseUrl: apiUrl });
    }
    try {
      const { username, password } = JSON.parse(stored);
      return new AntflyClient({
        baseUrl: apiUrl,
        auth: { username, password },
      });
    } catch {
      return new AntflyClient({ baseUrl: apiUrl });
    }
  }, [apiUrl]);

  // Fetch users list
  const fetchUsers = useCallback(async () => {
    try {
      setIsLoading(true);
      setError("");
      const data = await client.users.list();
      // Filter out any users with undefined usernames
      const validUsers = (data || []).filter(
        (user): user is UserListItem => typeof user.username === "string"
      );
      setUsers(validUsers);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load users");
    } finally {
      setIsLoading(false);
    }
  }, [client]);

  // Fetch permissions for selected user
  const fetchPermissions = useCallback(
    async (username: string) => {
      try {
        setError("");
        const data = await client.users.getPermissions(username);
        setPermissions((data as Permission[]) || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load permissions");
      }
    },
    [client]
  );

  // Create new user
  const handleCreateUser = async () => {
    setCreateError("");
    if (!newUsername || !newPassword) {
      setCreateError("Username and password are required");
      return;
    }

    try {
      await client.users.create(newUsername, {
        password: newPassword,
      });

      setCreateDialogOpen(false);
      setNewUsername("");
      setNewPassword("");
      fetchUsers();
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : "Failed to create user");
    }
  };

  // Delete user
  const handleDeleteUser = async (username: string) => {
    if (!confirm(`Are you sure you want to delete user "${username}"?`)) {
      return;
    }

    try {
      await client.users.delete(username);

      if (selectedUser === username) {
        setSelectedUser(null);
        setPermissions([]);
      }
      fetchUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete user");
    }
  };

  // Add permission
  const handleAddPermission = async () => {
    if (!selectedUser || !newPermResource) {
      return;
    }

    try {
      await client.users.addPermission(selectedUser, {
        resource: newPermResource,
        resource_type: newPermResourceType,
        type: newPermType,
      });

      setAddPermDialogOpen(false);
      setNewPermResource("");
      setNewPermResourceType("table");
      setNewPermType("read");
      fetchPermissions(selectedUser);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add permission");
    }
  };

  // Remove permission
  const handleRemovePermission = async (perm: Permission) => {
    if (!selectedUser) return;

    try {
      await client.users.removePermission(selectedUser, perm.resource, perm.resource_type);

      fetchPermissions(selectedUser);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove permission");
    }
  };

  // Change password
  const handleChangePassword = async () => {
    if (!selectedUser || !newPasswordValue) {
      setPasswordChangeError("Password is required");
      return;
    }

    try {
      await client.users.updatePassword(selectedUser, newPasswordValue);

      setChangePasswordDialogOpen(false);
      setNewPasswordValue("");
      setPasswordChangeError("");
    } catch (err) {
      setPasswordChangeError(err instanceof Error ? err.message : "Failed to change password");
    }
  };

  // Load users on mount
  useEffect(() => {
    fetchUsers();
  }, [fetchUsers]);

  // Load permissions when user is selected
  useEffect(() => {
    if (selectedUser) {
      fetchPermissions(selectedUser);
    }
  }, [selectedUser, fetchPermissions]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">User Management</h1>
          <p className="text-muted-foreground mt-1">Manage users and their permissions</p>
        </div>
        <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="mr-2 size-4" />
              Create User
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create New User</DialogTitle>
              <DialogDescription>
                Create a new user account with username and password.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4">
              {createError && (
                <Alert variant="destructive">
                  <p className="text-sm">{createError}</p>
                </Alert>
              )}
              <div className="space-y-2">
                <Label htmlFor="new-username">Username</Label>
                <Input
                  id="new-username"
                  value={newUsername}
                  onChange={(e) => setNewUsername(e.target.value)}
                  placeholder="Enter username"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="new-password">Password</Label>
                <Input
                  id="new-password"
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  placeholder="Enter password"
                />
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setCreateDialogOpen(false)}>
                Cancel
              </Button>
              <Button onClick={handleCreateUser}>Create User</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {error && (
        <Alert variant="destructive">
          <p className="text-sm">{error}</p>
        </Alert>
      )}

      <div className="grid gap-6 md:grid-cols-2">
        {/* Users List */}
        <Card>
          <CardHeader>
            <CardTitle>Users</CardTitle>
            <CardDescription>Select a user to view and manage their permissions</CardDescription>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <p className="text-muted-foreground">Loading users...</p>
            ) : users.length === 0 ? (
              <p className="text-muted-foreground">No users found</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Username</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {users.map((user) => (
                    <TableRow
                      key={user.username}
                      className={
                        selectedUser === user.username
                          ? "bg-muted"
                          : "cursor-pointer hover:bg-muted/50"
                      }
                      onClick={() => setSelectedUser(user.username)}
                    >
                      <TableCell className="font-medium">
                        {user.username}
                        {currentUser?.username === user.username && (
                          <Badge variant="secondary" className="ml-2">
                            You
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Dialog
                            open={changePasswordDialogOpen && selectedUser === user.username}
                            onOpenChange={(open) => {
                              if (open) setSelectedUser(user.username);
                              setChangePasswordDialogOpen(open);
                              if (!open) setPasswordChangeError("");
                            }}
                          >
                            <DialogTrigger asChild>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setSelectedUser(user.username);
                                }}
                              >
                                <Key className="size-4" />
                              </Button>
                            </DialogTrigger>
                            <DialogContent>
                              <DialogHeader>
                                <DialogTitle>Change Password</DialogTitle>
                                <DialogDescription>
                                  Change password for user {user.username}
                                </DialogDescription>
                              </DialogHeader>
                              <div className="space-y-4">
                                {passwordChangeError && (
                                  <Alert variant="destructive">
                                    <p className="text-sm">{passwordChangeError}</p>
                                  </Alert>
                                )}
                                <div className="space-y-2">
                                  <Label htmlFor="new-password-value">New Password</Label>
                                  <Input
                                    id="new-password-value"
                                    type="password"
                                    value={newPasswordValue}
                                    onChange={(e) => setNewPasswordValue(e.target.value)}
                                    placeholder="Enter new password"
                                  />
                                </div>
                              </div>
                              <DialogFooter>
                                <Button
                                  variant="outline"
                                  onClick={() => {
                                    setChangePasswordDialogOpen(false);
                                    setPasswordChangeError("");
                                  }}
                                >
                                  Cancel
                                </Button>
                                <Button onClick={handleChangePassword}>Change Password</Button>
                              </DialogFooter>
                            </DialogContent>
                          </Dialog>
                          {currentUser?.username !== user.username && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleDeleteUser(user.username);
                              }}
                            >
                              <Trash2 className="size-4 text-destructive" />
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        {/* Permissions Panel */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Shield className="size-5" />
              Permissions
              {selectedUser && (
                <span className="text-sm font-normal text-muted-foreground">
                  for {selectedUser}
                </span>
              )}
            </CardTitle>
            {selectedUser && <CardDescription>Manage permissions for this user</CardDescription>}
          </CardHeader>
          <CardContent>
            {!selectedUser ? (
              <p className="text-muted-foreground">Select a user to view permissions</p>
            ) : (
              <div className="space-y-4">
                <Dialog open={addPermDialogOpen} onOpenChange={setAddPermDialogOpen}>
                  <DialogTrigger asChild>
                    <Button variant="outline" size="sm" className="w-full">
                      <Plus className="mr-2 size-4" />
                      Add Permission
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Add Permission</DialogTitle>
                      <DialogDescription>
                        Grant a new permission to {selectedUser}
                      </DialogDescription>
                    </DialogHeader>
                    <div className="space-y-4">
                      <div className="space-y-2">
                        <Label htmlFor="perm-resource">Resource</Label>
                        <Input
                          id="perm-resource"
                          value={newPermResource}
                          onChange={(e) => setNewPermResource(e.target.value)}
                          placeholder='e.g., "table_name" or "*" for all'
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="perm-resource-type">Resource Type</Label>
                        <Select
                          value={newPermResourceType}
                          onValueChange={(value) =>
                            setNewPermResourceType(value as "table" | "user" | "*")
                          }
                        >
                          <SelectTrigger id="perm-resource-type">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="table">Table</SelectItem>
                            <SelectItem value="user">User</SelectItem>
                            <SelectItem value="*">All (*)</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="perm-type">Permission Type</Label>
                        <Select
                          value={newPermType}
                          onValueChange={(value) =>
                            setNewPermType(value as "read" | "write" | "admin")
                          }
                        >
                          <SelectTrigger id="perm-type">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="read">Read</SelectItem>
                            <SelectItem value="write">Write</SelectItem>
                            <SelectItem value="admin">Admin</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <DialogFooter>
                      <Button variant="outline" onClick={() => setAddPermDialogOpen(false)}>
                        Cancel
                      </Button>
                      <Button onClick={handleAddPermission}>Add Permission</Button>
                    </DialogFooter>
                  </DialogContent>
                </Dialog>

                {permissions.length === 0 ? (
                  <p className="text-muted-foreground text-sm">No permissions assigned</p>
                ) : (
                  <div className="space-y-2">
                    {permissions.map((perm) => (
                      <div
                        key={`${perm.type}-${perm.resource_type}-${perm.resource}`}
                        className="flex items-center justify-between rounded-lg border p-3"
                      >
                        <div className="space-y-1">
                          <div className="flex items-center gap-2">
                            <Badge variant="outline">{perm.type}</Badge>
                            <Badge variant="secondary">{perm.resource_type}</Badge>
                          </div>
                          <p className="text-sm font-mono">{perm.resource}</p>
                        </div>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleRemovePermission(perm)}
                        >
                          <Trash2 className="size-4 text-destructive" />
                        </Button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
